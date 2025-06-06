package main

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/gorilla/websocket"
)

// Configuration
const (
	BYBIT_WS_TRADE_URL  = "wss://stream.bybit.com/v5/trade"
	BYBIT_WS_PUBLIC_URL = "wss://stream.bybit.com/v5/public/linear"
	AUTO_CLOSE_DELAY    = 5 * time.Second
	ORDER_TIMEOUT       = 3 * time.Second
)

// Bybit credentials - SET THESE
var (
	BYBIT_API_KEY    = "o26xz3XaSZuVAr4XN9"
	BYBIT_API_SECRET = "5VXpP8FWyucDplRtECSKHiiR0f2SEPaQDTZA"
	TELEGRAM_TOKEN   = "7634313427:AAEDCWxF8xiSyKVKrc2fT5I3RCZ-5I-4Elk"
)

// Global variables
var (
	reqCounter int64
	bot        *tgbotapi.BotAPI
)

// Pre-compiled message templates for zero-allocation
var (
	authPrefix      = []byte(`{"op":"auth","args":["`)
	authMiddle1     = []byte(`",`)
	authMiddle2     = []byte(`,"`)
	authSuffix      = []byte(`"]}`)
	cancelPrefix    = []byte(`{"reqId":"`)
	cancelTemplate1 = []byte(`","header":{"X-BAPI-TIMESTAMP":"`)
	cancelTemplate2 = []byte(`","X-BAPI-RECV-WINDOW":"5000"},"op":"order.cancel","args":[{"symbol":"`)
	cancelTemplate3 = []byte(`","orderId":"`)
	cancelTemplate4 = []byte(`","category":"linear"}]}`)
	subPrefix       = []byte(`{"op":"subscribe","args":["tickers.`)
	subSuffix       = []byte(`"]}`)
)

// Structs
type FastPrice struct {
	Symbol string  `json:"symbol"`
	Price  float64 `json:"lastPrice,string"`
}

type TickerData struct {
	Topic string    `json:"topic"`
	Data  FastPrice `json:"data"`
}

type OrderResponse struct {
	ReqId   string `json:"reqId"`
	RetCode int    `json:"retCode"`
	RetMsg  string `json:"retMsg"`
	Data    struct {
		OrderId     string `json:"orderId"`
		OrderLinkId string `json:"orderLinkId"`
	} `json:"data"`
}

type CancelResponse struct {
	ReqId   string `json:"reqId"`
	RetCode int    `json:"retCode"`
	RetMsg  string `json:"retMsg"`
	Data    struct {
		OrderId     string `json:"orderId"`
		OrderLinkId string `json:"orderLinkId"`
	} `json:"data"`
}

type TradingBot struct {
	tradeConn   *websocket.Conn
	publicConn  *websocket.Conn
	priceCache  sync.Map
	connReady   int32
	publicReady int32
	responses   sync.Map
}

type TradeRequest struct {
	Symbol     string
	Side       string
	UsdtAmount float64
	Leverage   float64
	ChatId     int64
}

// Fast request ID generation
func generateReqId() string {
	counter := atomic.AddInt64(&reqCounter, 1)
	return "x" + strconv.FormatInt(counter, 36)
}

// Generate signature for authentication
func generateSignature(apiSecret string, timestamp int64) string {
	payload := fmt.Sprintf("GET/realtime%d", timestamp)
	h := hmac.New(sha256.New, []byte(apiSecret))
	h.Write([]byte(payload))
	return hex.EncodeToString(h.Sum(nil))
}

// Ultra-fast message building with zero allocations
func buildAuthMessage(apiKey, apiSecret string) []byte {
	timestamp := time.Now().UnixMilli()
	signature := generateSignature(apiSecret, timestamp)
	timestampStr := strconv.FormatInt(timestamp, 10)

	capacity := len(authPrefix) + len(apiKey) + len(authMiddle1) +
		len(timestampStr) + len(authMiddle2) + len(signature) + len(authSuffix)

	msg := make([]byte, 0, capacity)
	msg = append(msg, authPrefix...)
	msg = append(msg, []byte(apiKey)...)
	msg = append(msg, authMiddle1...)
	msg = append(msg, []byte(timestampStr)...)
	msg = append(msg, authMiddle2...)
	msg = append(msg, []byte(signature)...)
	msg = append(msg, authSuffix...)

	return msg
}

func buildOrderMessage(reqId, symbol, side, qty string) []byte {
	timestamp := strconv.FormatInt(time.Now().UnixMilli(), 10)

	// Build order with all required fields for Bybit v5
	orderJSON := fmt.Sprintf(`{"reqId":"%s","header":{"X-BAPI-TIMESTAMP":"%s","X-BAPI-RECV-WINDOW":"5000"},"op":"order.create","args":[{"symbol":"%s","side":"%s","orderType":"Market","qty":"%s","category":"linear","timeInForce":"IOC","reduceOnly":false}]}`,
		reqId, timestamp, symbol, side, qty)

	return []byte(orderJSON)
}

func buildCancelMessage(reqId, symbol, orderId string) []byte {
	timestamp := strconv.FormatInt(time.Now().UnixMilli(), 10)

	capacity := len(cancelPrefix) + len(reqId) + len(cancelTemplate1) +
		len(timestamp) + len(cancelTemplate2) + len(symbol) + len(cancelTemplate3) +
		len(orderId) + len(cancelTemplate4)

	msg := make([]byte, 0, capacity)
	msg = append(msg, cancelPrefix...)
	msg = append(msg, []byte(reqId)...)
	msg = append(msg, cancelTemplate1...)
	msg = append(msg, []byte(timestamp)...)
	msg = append(msg, cancelTemplate2...)
	msg = append(msg, []byte(symbol)...)
	msg = append(msg, cancelTemplate3...)
	msg = append(msg, []byte(orderId)...)
	msg = append(msg, cancelTemplate4...)

	return msg
}

func buildSubscribeMessage(symbol string) []byte {
	capacity := len(subPrefix) + len(symbol) + len(subSuffix)
	msg := make([]byte, 0, capacity)
	msg = append(msg, subPrefix...)
	msg = append(msg, []byte(symbol)...)
	msg = append(msg, subSuffix...)
	return msg
}

// Initialize trading bot
func NewTradingBot() *TradingBot {
	return &TradingBot{}
}

// Connect to WebSocket endpoints
func (tb *TradingBot) Connect() error {
	// Connect to trade WebSocket
	tradeConn, _, err := websocket.DefaultDialer.Dial(BYBIT_WS_TRADE_URL, nil)
	if err != nil {
		return fmt.Errorf("failed to connect to trade WebSocket: %v", err)
	}
	tb.tradeConn = tradeConn

	// Connect to public WebSocket
	publicConn, _, err := websocket.DefaultDialer.Dial(BYBIT_WS_PUBLIC_URL, nil)
	if err != nil {
		tradeConn.Close()
		return fmt.Errorf("failed to connect to public WebSocket: %v", err)
	}
	tb.publicConn = publicConn

	// Authenticate trade connection
	authMsg := buildAuthMessage(BYBIT_API_KEY, BYBIT_API_SECRET)
	if err := tb.tradeConn.WriteMessage(websocket.TextMessage, authMsg); err != nil {
		return fmt.Errorf("failed to authenticate: %v", err)
	}

	// Start message handlers
	go tb.handleTradeMessages()
	go tb.handlePublicMessages()

	// Wait for authentication
	time.Sleep(2 * time.Second)
	atomic.StoreInt32(&tb.connReady, 1)
	atomic.StoreInt32(&tb.publicReady, 1)

	return nil
}

// Handle trade WebSocket messages
func (tb *TradingBot) handleTradeMessages() {
	defer tb.tradeConn.Close()

	for {
		_, message, err := tb.tradeConn.ReadMessage()
		if err != nil {
			log.Printf("Trade WebSocket read error: %v", err)
			atomic.StoreInt32(&tb.connReady, 0)
			return
		}

		// Ultra-fast message type detection
		if len(message) > 20 && bytes.Contains(message, []byte(`"reqId"`)) {
			if bytes.Contains(message, []byte(`"order.create"`)) {
				var resp OrderResponse
				if json.Unmarshal(message, &resp) == nil && resp.ReqId != "" {
					if respChan, ok := tb.responses.Load(resp.ReqId); ok {
						select {
						case respChan.(chan OrderResponse) <- resp:
						default:
						}
					}
				}
			} else if bytes.Contains(message, []byte(`"order.cancel"`)) {
				var resp CancelResponse
				if json.Unmarshal(message, &resp) == nil && resp.ReqId != "" {
					if respChan, ok := tb.responses.Load(resp.ReqId); ok {
						select {
						case respChan.(chan CancelResponse) <- resp:
						default:
						}
					}
				}
			}
		}
	}
}

// Handle public WebSocket messages with improved error handling
func (tb *TradingBot) handlePublicMessages() {
	defer tb.publicConn.Close()

	for {
		_, message, err := tb.publicConn.ReadMessage()
		if err != nil {
			log.Printf("Public WebSocket read error: %v", err)
			atomic.StoreInt32(&tb.publicReady, 0)
			return
		}

		// Debug: Log raw messages to understand the format
		log.Printf("📡 Received message: %s", string(message))

		// Ultra-fast ticker detection with multiple patterns
		if len(message) > 30 {
			// Check for different ticker message patterns
			if bytes.Contains(message, []byte(`"tickers"`)) ||
				bytes.Contains(message, []byte(`"topic"`)) {

				// Try multiple parsing approaches
				tb.parseTickerMessage(message)
			}
		}
	}
}

// Enhanced ticker parsing with fallback methods
func (tb *TradingBot) parseTickerMessage(message []byte) {
	// Method 1: Try the standard TickerData structure
	var tickerData TickerData
	if json.Unmarshal(message, &tickerData) == nil {
		if tickerData.Data.Symbol != "" && tickerData.Data.Price > 0 {
			tb.priceCache.Store(tickerData.Data.Symbol, tickerData.Data.Price)
			log.Printf("✅ Price updated: %s = %.8f", tickerData.Data.Symbol, tickerData.Data.Price)
			return
		}
	}

	// Method 2: Try alternative ticker structure
	var altTicker struct {
		Topic string `json:"topic"`
		Type  string `json:"type"`
		Data  struct {
			Symbol     string `json:"symbol"`
			LastPrice  string `json:"lastPrice"`
			MarkPrice  string `json:"markPrice"`
			IndexPrice string `json:"indexPrice"`
			Bid1Price  string `json:"bid1Price"`
			Ask1Price  string `json:"ask1Price"`
		} `json:"data"`
	}

	if json.Unmarshal(message, &altTicker) == nil {
		symbol := altTicker.Data.Symbol
		if symbol != "" {
			// Try different price fields in order of preference
			var price float64
			var err error
			var priceSource string

			if altTicker.Data.LastPrice != "" && altTicker.Data.LastPrice != "0" {
				price, err = strconv.ParseFloat(altTicker.Data.LastPrice, 64)
				priceSource = "lastPrice"
			} else if altTicker.Data.MarkPrice != "" && altTicker.Data.MarkPrice != "0" {
				price, err = strconv.ParseFloat(altTicker.Data.MarkPrice, 64)
				priceSource = "markPrice"
			} else if altTicker.Data.Bid1Price != "" && altTicker.Data.Bid1Price != "0" {
				price, err = strconv.ParseFloat(altTicker.Data.Bid1Price, 64)
				priceSource = "bid1Price"
			} else if altTicker.Data.Ask1Price != "" && altTicker.Data.Ask1Price != "0" {
				price, err = strconv.ParseFloat(altTicker.Data.Ask1Price, 64)
				priceSource = "ask1Price"
			} else if altTicker.Data.IndexPrice != "" && altTicker.Data.IndexPrice != "0" {
				price, err = strconv.ParseFloat(altTicker.Data.IndexPrice, 64)
				priceSource = "indexPrice"
			}

			if err == nil && price > 0 {
				tb.priceCache.Store(symbol, price)
				log.Printf("✅ Price updated (%s): %s = %.8f", priceSource, symbol, price)
				return
			}
		}
	}

	// Method 3: Generic JSON parsing for debugging
	var genericData map[string]interface{}
	if json.Unmarshal(message, &genericData) == nil {
		log.Printf("🔍 Message structure: %+v", genericData)
	}
}

// Enhanced subscription with confirmation
func (tb *TradingBot) SubscribeToSymbol(symbol string) error {
	if atomic.LoadInt32(&tb.publicReady) != 1 {
		return fmt.Errorf("public connection not ready")
	}

	log.Printf("🔔 Subscribing to %s...", symbol)
	subMsg := buildSubscribeMessage(symbol)

	if err := tb.publicConn.WriteMessage(websocket.TextMessage, subMsg); err != nil {
		return err
	}

	// Wait a moment for subscription confirmation
	time.Sleep(500 * time.Millisecond)

	// Check if we received price data
	if _, exists := tb.priceCache.Load(symbol); exists {
		log.Printf("✅ %s price data confirmed", symbol)
	} else {
		log.Printf("⚠️ %s subscription sent, waiting for price data...", symbol)
	}

	return nil
}

// Enhanced price retrieval with debug info
func (tb *TradingBot) GetPrice(symbol string) (float64, error) {
	if price, ok := tb.priceCache.Load(symbol); ok {
		priceFloat := price.(float64)
		log.Printf("💰 Retrieved price for %s: %.8f", symbol, priceFloat)
		return priceFloat, nil
	}

	// Debug: Show what symbols we have
	log.Printf("❌ No price for %s. Available symbols:", symbol)
	tb.priceCache.Range(func(key, value interface{}) bool {
		log.Printf("   - %s: %.8f", key.(string), value.(float64))
		return true
	})

	return 0, fmt.Errorf("price not available for %s", symbol)
}

// Enhanced quantity calculation with proper formatting
func formatQuantity(qty float64, symbol string) string {
	// Round to nearest integer for quantities >= 10
	if qty >= 10 {
		return fmt.Sprintf("%.0f", math.Round(qty))
	}

	// For smaller quantities, use appropriate precision
	if qty >= 1 {
		return fmt.Sprintf("%.1f", math.Round(qty*10)/10)
	}

	// For very small quantities
	return fmt.Sprintf("%.3f", math.Round(qty*1000)/1000)
}

// Enhanced trade execution with better validation
func (tb *TradingBot) ExecuteTrade(req TradeRequest) {
	startTime := time.Now()

	// Check connections
	if atomic.LoadInt32(&tb.connReady) != 1 {
		sendMessage(req.ChatId, "❌ Trade connection not ready")
		return
	}

	// Get current price
	price, err := tb.GetPrice(req.Symbol)
	if err != nil {
		sendMessage(req.ChatId, fmt.Sprintf("❌ Price not available for %s. Use /sub %s first", req.Symbol, req.Symbol))
		return
	}

	// Calculate quantity with validation
	positionValue := req.UsdtAmount * req.Leverage
	qty := positionValue / price

	// Validate minimum order size (typically 5-10 USDT minimum)
	if positionValue < 5.0 {
		sendMessage(req.ChatId, fmt.Sprintf("❌ Minimum order value is 5 USDT (you specified %.2f USDT)", positionValue))
		return
	}

	// Format quantity properly
	qtyStr := formatQuantity(qty, req.Symbol)

	// Parse back to check if it's valid
	parsedQty, parseErr := strconv.ParseFloat(qtyStr, 64)
	if parseErr != nil || parsedQty <= 0 {
		sendMessage(req.ChatId, fmt.Sprintf("❌ Invalid quantity format: %s", qtyStr))
		return
	}

	// Additional validation for very small quantities
	if parsedQty < 0.1 {
		sendMessage(req.ChatId, fmt.Sprintf("❌ Quantity too small: %s (minimum ~0.1)", qtyStr))
		return
	}

	log.Printf("📊 Trade calculation: Symbol=%s, Price=%.8f, Value=%.2f, Qty=%s (%.6f)",
		req.Symbol, price, positionValue, qtyStr, parsedQty)

	// Create order
	reqId := generateReqId()
	orderMsg := buildOrderMessage(reqId, req.Symbol, req.Side, qtyStr)

	// Log the order message for debugging
	log.Printf("📤 Sending order: %s", string(orderMsg))

	// Setup response channel
	respChan := make(chan OrderResponse, 1)
	tb.responses.Store(reqId, respChan)
	defer tb.responses.Delete(reqId)

	// Send order
	orderStartTime := time.Now()
	if err := tb.tradeConn.WriteMessage(websocket.TextMessage, orderMsg); err != nil {
		sendMessage(req.ChatId, fmt.Sprintf("❌ Failed to send order: %v", err))
		return
	}

	// Wait for response
	select {
	case resp := <-respChan:
		orderDuration := time.Since(orderStartTime)

		if resp.RetCode != 0 {
			sendMessage(req.ChatId, fmt.Sprintf("❌ Order failed: %s\n🔍 Debug: Qty=%s (%.6f), Value=%.2f USDT, Price=%.8f",
				resp.RetMsg, qtyStr, parsedQty, positionValue, price))
			return
		}

		orderId := resp.Data.OrderId

		// Send success message
		msg := fmt.Sprintf("🚀 ULTRA-FAST EXECUTION!\n"+
			"📊 %s %s %.2f USDT %.0fx\n"+
			"⚡ Open: %.3f ms\n"+
			"🆔 Order: %s\n"+
			"💰 Price: %.8f\n"+
			"📈 Qty: %s\n\n"+
			"⏳ Auto-closing in 5 seconds...",
			req.Symbol, req.Side, req.UsdtAmount, req.Leverage,
			float64(orderDuration.Nanoseconds())/1000000.0,
			orderId, price, qtyStr)

		sendMessage(req.ChatId, msg)

		// Wait 5 seconds then close
		time.Sleep(AUTO_CLOSE_DELAY)

		// Close position
		closeStartTime := time.Now()
		oppositeSide := "Sell"
		if req.Side == "Sell" {
			oppositeSide = "Buy"
		}

		// Try to cancel first, then place opposite order
		tb.closePosition(req.ChatId, req.Symbol, orderId, oppositeSide, qtyStr, closeStartTime)

		totalDuration := time.Since(startTime)
		sendMessage(req.ChatId, fmt.Sprintf("⏱️ Total: %.3f ms", float64(totalDuration.Nanoseconds())/1000000.0))

	case <-time.After(ORDER_TIMEOUT):
		sendMessage(req.ChatId, "❌ Order timeout")
		return
	}
}

// Close position by canceling or placing opposite order
func (tb *TradingBot) closePosition(chatId int64, symbol, orderId, oppositeSide, qty string, startTime time.Time) {
	// Try to cancel first
	cancelReqId := generateReqId()
	cancelMsg := buildCancelMessage(cancelReqId, symbol, orderId)

	cancelRespChan := make(chan CancelResponse, 1)
	tb.responses.Store(cancelReqId, cancelRespChan)
	defer tb.responses.Delete(cancelReqId)

	tb.tradeConn.WriteMessage(websocket.TextMessage, cancelMsg)

	select {
	case cancelResp := <-cancelRespChan:
		closeDuration := time.Since(startTime)
		if cancelResp.RetCode == 0 {
			sendMessage(chatId, fmt.Sprintf("✅ Cancelled successfully!\n⚡ Cancel: %.3f ms",
				float64(closeDuration.Nanoseconds())/1000000.0))
		} else {
			// Cancel failed, place opposite order
			tb.placeOppositeOrder(chatId, symbol, oppositeSide, qty, startTime)
		}
	case <-time.After(2 * time.Second):
		// Cancel timeout, place opposite order
		tb.placeOppositeOrder(chatId, symbol, oppositeSide, qty, startTime)
	}
}

// Place opposite order to close position
func (tb *TradingBot) placeOppositeOrder(chatId int64, symbol, side, qty string, startTime time.Time) {
	closeReqId := generateReqId()
	closeMsg := buildOrderMessage(closeReqId, symbol, side, qty)

	closeRespChan := make(chan OrderResponse, 1)
	tb.responses.Store(closeReqId, closeRespChan)
	defer tb.responses.Delete(closeReqId)

	tb.tradeConn.WriteMessage(websocket.TextMessage, closeMsg)

	select {
	case closeResp := <-closeRespChan:
		closeDuration := time.Since(startTime)
		if closeResp.RetCode == 0 {
			sendMessage(chatId, fmt.Sprintf("🔄 Closed with opposite order!\n⚡ Close: %.3f ms",
				float64(closeDuration.Nanoseconds())/1000000.0))
		} else {
			sendMessage(chatId, fmt.Sprintf("❌ Close failed: %s", closeResp.RetMsg))
		}
	case <-time.After(2 * time.Second):
		sendMessage(chatId, "❌ Close timeout")
	}
}

// Telegram message handler
func sendMessage(chatId int64, text string) {
	msg := tgbotapi.NewMessage(chatId, text)
	bot.Send(msg)
}

// Handle Telegram updates
func handleUpdates(tb *TradingBot) {
	u := tgbotapi.NewUpdate(0)
	u.Timeout = 60

	updates := bot.GetUpdatesChan(u)

	for update := range updates {
		if update.Message == nil {
			continue
		}

		chatId := update.Message.Chat.ID
		text := update.Message.Text

		if update.Message.IsCommand() {
			switch update.Message.Command() {
			case "start":
				msg := "🚀 Ultra-Fast Bybit Trading Bot\n\n" +
					"Commands:\n" +
					"/sub <SYMBOL> - Subscribe to price updates\n" +
					"/trade <SYMBOL> <BUY/SELL> <USDT> <LEVERAGE> - Execute trade\n" +
					"/status - Connection status\n" +
					"/prices - Show cached prices\n" +
					"/debug <SYMBOL> - Debug symbol info\n\n" +
					"Example: /trade BTCUSDT BUY 10 2"
				sendMessage(chatId, msg)

			case "debug":
				args := strings.Fields(text)
				if len(args) != 2 {
					sendMessage(chatId, "Usage: /debug <SYMBOL>\nExample: /debug SCAUSDT")
					continue
				}

				symbol := strings.ToUpper(args[1])

				// Check price cache
				if price, exists := tb.priceCache.Load(symbol); exists {
					priceFloat := price.(float64)

					// Test calculation
					testValue := 6.0 * 2.0 // 6 USDT * 2x leverage
					testQty := testValue / priceFloat
					testQtyStr := formatQuantity(testQty, symbol)

					msg := fmt.Sprintf("🔍 Debug %s:\n"+
						"💰 Cached Price: %.8f\n"+
						"📊 Test Calc (6 USDT * 2x):\n"+
						"   Value: %.2f USDT\n"+
						"   Quantity: %s\n"+
						"   Raw Qty: %.12f",
						symbol, priceFloat, testValue, testQtyStr, testQty)
					sendMessage(chatId, msg)
				} else {
					sendMessage(chatId, fmt.Sprintf("❌ No price data for %s", symbol))
				}

			case "prices":
				var priceList []string
				tb.priceCache.Range(func(key, value interface{}) bool {
					symbol := key.(string)
					price := value.(float64)
					priceList = append(priceList, fmt.Sprintf("%s: %.8f", symbol, price))
					return true
				})

				if len(priceList) == 0 {
					sendMessage(chatId, "📊 No cached prices available")
				} else {
					msg := "📊 Cached Prices:\n" + strings.Join(priceList, "\n")
					sendMessage(chatId, msg)
				}

			case "status":
				tradeStatus := "❌ Disconnected"
				publicStatus := "❌ Disconnected"

				if atomic.LoadInt32(&tb.connReady) == 1 {
					tradeStatus = "✅ Connected"
				}
				if atomic.LoadInt32(&tb.publicReady) == 1 {
					publicStatus = "✅ Connected"
				}

				// Count cached prices
				priceCount := 0
				tb.priceCache.Range(func(k, v interface{}) bool {
					priceCount++
					return true
				})

				msg := fmt.Sprintf("📡 Connection Status:\n"+
					"Trade: %s\n"+
					"Public: %s\n"+
					"💰 Cached Prices: %d", tradeStatus, publicStatus, priceCount)
				sendMessage(chatId, msg)

			case "sub":
				args := strings.Fields(text)
				if len(args) != 2 {
					sendMessage(chatId, "Usage: /sub <SYMBOL>\nExample: /sub BTCUSDT")
					continue
				}

				symbol := strings.ToUpper(args[1])
				if err := tb.SubscribeToSymbol(symbol); err != nil {
					sendMessage(chatId, fmt.Sprintf("❌ Failed to subscribe: %v", err))
				} else {
					sendMessage(chatId, fmt.Sprintf("🔥 Subscribed to %s price updates", symbol))

					// Wait and check for price data
					time.Sleep(2 * time.Second)
					if _, exists := tb.priceCache.Load(symbol); exists {
						if price, err := tb.GetPrice(symbol); err == nil {
							sendMessage(chatId, fmt.Sprintf("✅ %s price confirmed: %.8f", symbol, price))
						}
					} else {
						sendMessage(chatId, fmt.Sprintf("⚠️ %s subscription sent, but no price data received yet. Try /prices to see available data.", symbol))
					}
				}

			case "trade":
				args := strings.Fields(text)
				if len(args) != 5 {
					sendMessage(chatId, "Usage: /trade <SYMBOL> <BUY/SELL> <USDT> <LEVERAGE>\n"+
						"Example: /trade BTCUSDT BUY 10 2")
					continue
				}

				symbol := strings.ToUpper(args[1])
				side := strings.Title(strings.ToLower(args[2]))

				if side != "Buy" && side != "Sell" {
					sendMessage(chatId, "❌ Side must be BUY or SELL")
					continue
				}

				usdtAmount, err := strconv.ParseFloat(args[3], 64)
				if err != nil || usdtAmount <= 0 {
					sendMessage(chatId, "❌ Invalid USDT amount")
					continue
				}

				leverage, err := strconv.ParseFloat(args[4], 64)
				if err != nil || leverage <= 0 {
					sendMessage(chatId, "❌ Invalid leverage")
					continue
				}

				req := TradeRequest{
					Symbol:     symbol,
					Side:       side,
					UsdtAmount: usdtAmount,
					Leverage:   leverage,
					ChatId:     chatId,
				}

				go tb.ExecuteTrade(req)
			}
		}
	}
}

func main() {
	// Initialize Telegram bot
	var err error
	bot, err = tgbotapi.NewBotAPI(TELEGRAM_TOKEN)
	if err != nil {
		log.Fatal("Failed to create Telegram bot:", err)
	}

	log.Printf("Authorized on account %s", bot.Self.UserName)

	// Initialize trading bot
	tb := NewTradingBot()
	if err := tb.Connect(); err != nil {
		log.Fatal("Failed to connect to Bybit:", err)
	}

	log.Println("🚀 Ultra-Fast Bybit Trading Bot started successfully!")

	// Handle Telegram updates
	handleUpdates(tb)
}

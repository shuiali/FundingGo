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

const (
	FUNDING_HOLD_DELAY = 1000 * time.Millisecond // Exactly 1000ms
)
const (
	BYBIT_WS_TRADE_URL  = "wss://stream.bybit.com/v5/trade"
	BYBIT_WS_PUBLIC_URL = "wss://stream.bybit.com/v5/public/linear"
	AUTO_CLOSE_DELAY    = 5000 * time.Millisecond // Changed to exactly 5000ms
	ORDER_TIMEOUT       = 3 * time.Second

	// Funding constants
	FUNDING_OPEN_DELAY  = 1000 * time.Millisecond // 1 second before funding
	FUNDING_CLOSE_DELAY = 100 * time.Millisecond  // 100ms after funding

	FUNDING_TOPIC_PREFIX = "funding."
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
	cancelPrefix    = []byte(`{"reqId":"`)
	cancelTemplate1 = []byte(`","header":{"X-BAPI-TIMESTAMP":"`)
	cancelTemplate2 = []byte(`","X-BAPI-RECV-WINDOW":"10000"},"op":"order.cancel","args":[{"symbol":"`) // Increased to 10000
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

type FundingInfo struct {
	Symbol           string  `json:"symbol"`
	FundingRate      float64 `json:"fundingRate,string"`
	NextFundingTime  string  `json:"nextFundingTime"`
	FundingTimestamp int64   // Unix timestamp
}

type FundingData struct {
	Topic string `json:"topic"`
	Type  string `json:"type"`
	Data  struct {
		Symbol          string `json:"symbol"`
		FundingRate     string `json:"fundingRate"`
		NextFundingTime string `json:"nextFundingTime"`
	} `json:"data"`
}

type TradingBot struct {
	tradeConn    *websocket.Conn
	publicConn   *websocket.Conn
	priceCache   sync.Map
	fundingCache sync.Map
	strategies   sync.Map
	connReady    int32
	publicReady  int32
	responses    sync.Map
}

type TradeRequest struct {
	Symbol     string
	Side       string
	UsdtAmount float64
	Leverage   float64
	ChatId     int64
}

type FundingStrategy struct {
	Symbol      string
	UsdtAmount  float64
	Leverage    float64
	ChatId      int64
	NextFunding time.Time
	FundingRate float64
	TargetSide  string
}

// Fast request ID generation
func generateReqId() string {
	counter := atomic.AddInt64(&reqCounter, 1)
	return "x" + strconv.FormatInt(counter, 36)
}

// Fix the signature generation - use expiry timestamp, not current timestamp
func generateSignature(apiSecret string, expiryTimestamp int64) string {
	// For WebSocket authentication, use the expiry timestamp in the signature
	message := fmt.Sprintf("GET/realtime%d", expiryTimestamp)
	h := hmac.New(sha256.New, []byte(apiSecret))
	h.Write([]byte(message))
	signature := hex.EncodeToString(h.Sum(nil))

	log.Printf("🔑 Signature generation - Message: %s, Signature: %s", message, signature)
	return signature
}

// Fix the buildAuthMessage function to use expiry timestamp for signature
func buildAuthMessage(apiKey, apiSecret string) []byte {
	// Use current timestamp in milliseconds
	timestamp := time.Now().UnixMilli()

	// For WebSocket auth, use timestamp + recv_window as expiry
	expiryTimestamp := timestamp + 5000 // 5 seconds from now

	// Generate signature using the EXPIRY timestamp (this is the key fix!)
	signature := generateSignature(apiSecret, expiryTimestamp)

	// The auth message format: ["api_key", expiry_timestamp, "signature"]
	authJSON := fmt.Sprintf(`{"op":"auth","args":["%s",%d,"%s"]}`,
		apiKey, expiryTimestamp, signature)

	log.Printf("🔐 Auth details - API Key: %s, Current: %d, Expiry: %d, Signature: %s",
		apiKey, timestamp, expiryTimestamp, signature)
	log.Printf("🔐 Signature payload: GET/realtime%d", expiryTimestamp)

	return []byte(authJSON)
}

// Add test function to verify with documentation example
func testSignatureWithDocExample() {
	apiSecret := BYBIT_API_SECRET

	// Test with the exact example from documentation
	testApiKey := "XXXXXX"
	testExpiry := int64(1711010121452)
	expectedSignature := "ec71040eff72b163a36153d770b69d6637bcb29348fbfbb16c269a76595ececf"

	// Generate signature using test values
	testMessage := fmt.Sprintf("GET/realtime%d", testExpiry)
	h := hmac.New(sha256.New, []byte(apiSecret))
	h.Write([]byte(testMessage))
	actualSignature := hex.EncodeToString(h.Sum(nil))

	log.Printf("🧪 Test with doc example:")
	log.Printf("    API Key: %s", testApiKey)
	log.Printf("    Expiry: %d", testExpiry)
	log.Printf("    Message: %s", testMessage)
	log.Printf("    Expected: %s", expectedSignature)
	log.Printf("    Actual: %s", actualSignature)
	log.Printf("    Match: %t", actualSignature == expectedSignature)
}

// Test both signature methods
func testBothSignatureMethods() {
	apiSecret := BYBIT_API_SECRET
	testExpiry := int64(1711010121452)

	// Method 1: GET/realtime + timestamp
	payload1 := fmt.Sprintf("GET/realtime%d", testExpiry)
	h1 := hmac.New(sha256.New, []byte(apiSecret))
	h1.Write([]byte(payload1))
	sig1 := hex.EncodeToString(h1.Sum(nil))
	log.Printf("🧪 Method 1 (GET/realtime): %s -> %s", payload1, sig1)

	// Method 2: Just timestamp
	payload2 := fmt.Sprintf("%d", testExpiry)
	h2 := hmac.New(sha256.New, []byte(apiSecret))
	h2.Write([]byte(payload2))
	sig2 := hex.EncodeToString(h2.Sum(nil))
	log.Printf("🧪 Method 2 (timestamp only): %s -> %s", payload2, sig2)

	// Method 3: API key + timestamp
	payload3 := fmt.Sprintf("%s%d", BYBIT_API_KEY, testExpiry)
	h3 := hmac.New(sha256.New, []byte(apiSecret))
	h3.Write([]byte(payload3))
	sig3 := hex.EncodeToString(h3.Sum(nil))
	log.Printf("🧪 Method 3 (key+timestamp): %s -> %s", payload3, sig3)

	log.Printf("🧪 Expected from docs: ec71040eff72b163a36153d770b69d6637bcb29348fbfbb16c269a76595ececf")
}

// Initialize trading bot
func NewTradingBot() *TradingBot {
	return &TradingBot{}
}

// Connect to WebSocket endpoints
func (tb *TradingBot) Connect() error {
	// Test signature generation first
	testSignatureWithDocExample()

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

	// Start message handlers BEFORE authentication
	go tb.handleTradeMessages()
	go tb.handlePublicMessages()

	// Authenticate trade connection
	authMsg := buildAuthMessage(BYBIT_API_KEY, BYBIT_API_SECRET)
	log.Printf("🔐 Sending auth message: %s", string(authMsg))

	if err := tb.tradeConn.WriteMessage(websocket.TextMessage, authMsg); err != nil {
		return fmt.Errorf("failed to authenticate: %v", err)
	}

	// Wait for authentication response
	log.Printf("⏳ Waiting for authentication...")
	time.Sleep(3 * time.Second)

	// Set public ready immediately since it doesn't need auth
	atomic.StoreInt32(&tb.publicReady, 1)

	log.Printf("✅ Connection setup complete")
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

		log.Printf("📡 Received trade message: %s", string(message))

		// Handle authentication response
		if bytes.Contains(message, []byte(`"op":"auth"`)) {
			var authResp struct {
				RetCode int    `json:"retCode"`
				RetMsg  string `json:"retMsg"`
				Op      string `json:"op"`
				ConnId  string `json:"connId"`
			}

			if err := json.Unmarshal(message, &authResp); err == nil {
				if authResp.RetCode == 0 {
					log.Printf("✅ Authentication successful! ConnId: %s", authResp.ConnId)
					atomic.StoreInt32(&tb.connReady, 1)
				} else {
					log.Printf("❌ Authentication failed! Code: %d, Message: %s", authResp.RetCode, authResp.RetMsg)
					return
				}
			}
			continue
		}

		// Ultra-fast message type detection for order responses
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

		// Process ticker messages (both snapshot and delta updates)
		if bytes.Contains(message, []byte(`"topic":"tickers.`)) {
			// Log ticker messages with funding data for debugging
			if bytes.Contains(message, []byte(`"fundingRate"`)) {
				log.Printf("📡 Ticker with funding: %s", string(message))
			}

			// Parse both price and funding data from the message
			var tickerData struct {
				Topic string `json:"topic"`
				Type  string `json:"type"`
				Data  struct {
					Symbol          string `json:"symbol"`
					LastPrice       string `json:"lastPrice"`
					MarkPrice       string `json:"markPrice"`
					NextFundingTime string `json:"nextFundingTime"`
					FundingRate     string `json:"fundingRate"`
				} `json:"data"`
			}

			if err := json.Unmarshal(message, &tickerData); err == nil {
				symbol := tickerData.Data.Symbol

				// If symbol is empty but we have topic, extract symbol from topic
				if symbol == "" && strings.HasPrefix(tickerData.Topic, "tickers.") {
					symbol = strings.TrimPrefix(tickerData.Topic, "tickers.")
				}

				if symbol != "" {
					// Process price data - use lastPrice if available, otherwise markPrice
					if tickerData.Data.LastPrice != "" {
						if price, err := strconv.ParseFloat(tickerData.Data.LastPrice, 64); err == nil && price > 0 {
							tb.priceCache.Store(symbol, price)
							log.Printf("💰 Price updated: %s = %.8f", symbol, price)
						}
					} else if tickerData.Data.MarkPrice != "" {
						// Fallback to mark price if last price isn't available
						if price, err := strconv.ParseFloat(tickerData.Data.MarkPrice, 64); err == nil && price > 0 {
							tb.priceCache.Store(symbol, price)
							log.Printf("💰 Mark price updated: %s = %.8f", symbol, price)
						}
					}

					// Process funding data if available
					if tickerData.Data.FundingRate != "" {
						rate, err := strconv.ParseFloat(tickerData.Data.FundingRate, 64)
						if err == nil {
							// Process next funding time if available
							var nextFundingTime time.Time
							var fundingTimestamp int64

							if tickerData.Data.NextFundingTime != "" {
								fundingTimeMs, err := strconv.ParseInt(tickerData.Data.NextFundingTime, 10, 64)
								if err == nil {
									nextFundingTime = time.Unix(fundingTimeMs/1000, 0)
									fundingTimestamp = nextFundingTime.Unix()
								}
							}

							// Only store if we have a valid funding time
							if !nextFundingTime.IsZero() {
								info := FundingInfo{
									Symbol:           symbol,
									FundingRate:      rate,
									NextFundingTime:  nextFundingTime.Format("2006-01-02T15:04:05Z"),
									FundingTimestamp: fundingTimestamp,
								}

								tb.fundingCache.Store(symbol, info)
								log.Printf("💰 Updated funding for %s: Rate=%.6f%%, Next=%s",
									symbol, rate*100, nextFundingTime.Format("15:04:05 MST"))

								// Update any active strategies for this symbol
								if strategy, ok := tb.strategies.Load(symbol); ok {
									tb.updateFundingStrategy(strategy.(*FundingStrategy), info)
								}
							}
						}
					}
				}
			}
		}
	}
}

// Enhanced ticker parsing with fallback methods
func (tb *TradingBot) parseTickerMessage(message []byte) {
	var tickerData struct {
		Topic string `json:"topic"`
		Type  string `json:"type"`
		Data  struct {
			Symbol          string `json:"symbol"`
			LastPrice       string `json:"lastPrice"`
			NextFundingTime string `json:"nextFundingTime"`
			FundingRate     string `json:"fundingRate"`
			MarkPrice       string `json:"markPrice"`
		} `json:"data"`
	}

	if err := json.Unmarshal(message, &tickerData); err == nil && tickerData.Data.Symbol != "" {
		symbol := tickerData.Data.Symbol

		// Always try to parse and store price first
		if price, err := strconv.ParseFloat(tickerData.Data.LastPrice, 64); err == nil && price > 0 {
			tb.priceCache.Store(symbol, price)
			log.Printf("💰 Price updated: %s = %.8f", symbol, price)
		}

		// Then parse funding data if available
		if tickerData.Data.FundingRate != "" && tickerData.Data.NextFundingTime != "" {
			rate, err := strconv.ParseFloat(tickerData.Data.FundingRate, 64)
			if err == nil {
				fundingTimeMs, err := strconv.ParseInt(tickerData.Data.NextFundingTime, 10, 64)
				if err == nil {
					nextTime := time.Unix(fundingTimeMs/1000, 0)

					info := FundingInfo{
						Symbol:           symbol,
						FundingRate:      rate,
						NextFundingTime:  nextTime.Format("2006-01-02T15:04:05Z"),
						FundingTimestamp: nextTime.Unix(),
					}

					tb.fundingCache.Store(symbol, info)
					log.Printf("💰 Updated funding for %s: Rate=%.6f%%, Next=%s",
						symbol, rate*100, nextTime.Format("15:04:05 MST"))
				}
			}
		}
		return // Successfully parsed main format
	}

	// Only try fallbacks if main parsing failed
	log.Printf("📡 Using fallback parser for: %s", string(message))

	// Try simpler format
	var simpleData struct {
		Topic string `json:"topic"`
		Data  struct {
			Symbol    string `json:"symbol"`
			LastPrice string `json:"lastPrice"`
		} `json:"data"`
	}
	if err := json.Unmarshal(message, &simpleData); err == nil && simpleData.Data.Symbol != "" {
		if price, err := strconv.ParseFloat(simpleData.Data.LastPrice, 64); err == nil && price > 0 {
			tb.priceCache.Store(simpleData.Data.Symbol, price)
			log.Printf("💰 Price updated (simple): %s = %.8f", simpleData.Data.Symbol, price)
		}
	}
}

// Remove unused variables and functions to clean up warnings
// Remove these unused variables:
// var (
//     authPrefix      = []byte(`{"op":"auth","args":["`)
//     authMiddle1     = []byte(`",`)
//     authMiddle2     = []byte(`,"`)
//     authSuffix      = []byte(`"]}`)
// )

// Remove unused functions
// func buildOrderMessage(reqId, symbol, side, qty string) []byte {
// }

// func buildCancelMessage(reqId, symbol, orderId string) []byte {
// }

// func buildSubscribeMessage(symbol string) []byte {
// }

// Update formatQuantity to remove unused parameter
func formatQuantity(qty float64) string {
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

// Add this function after the formatQuantity function
func formatDuration(d time.Duration) string {
	if d < 0 {
		return "already passed"
	}

	hours := int(d.Hours())
	minutes := int(d.Minutes()) % 60
	seconds := int(d.Seconds()) % 60

	if hours > 0 {
		return fmt.Sprintf("%dh %dm %ds", hours, minutes, seconds)
	} else if minutes > 0 {
		return fmt.Sprintf("%dm %ds", minutes, seconds)
	} else {
		return fmt.Sprintf("%ds", seconds)
	}
}

func (tb *TradingBot) ultraFastClose(chatId int64, symbol, orderId string,
	cancelReqId, closeReqId string, cancelMsg, closeMsg []byte,
	startTime time.Time, actualDelay time.Duration) {

	// Setup response channels for both operations
	cancelRespChan := make(chan CancelResponse, 1)
	closeRespChan := make(chan OrderResponse, 1)
	tb.responses.Store(cancelReqId, cancelRespChan)
	tb.responses.Store(closeReqId, closeRespChan)
	defer func() {
		tb.responses.Delete(cancelReqId)
		tb.responses.Delete(closeReqId)
	}()

	// Send cancel immediately
	tb.tradeConn.WriteMessage(websocket.TextMessage, cancelMsg)

	// Wait very briefly for cancel response
	select {
	case cancelResp := <-cancelRespChan:
		closeDuration := time.Since(startTime)
		if cancelResp.RetCode == 0 {
			// Cancel successful
			go sendMessage(chatId, fmt.Sprintf("✅ Cancelled successfully!\n⚡ Cancel: %.3f ms\n🕐 Opened for: %.0f ms",
				float64(closeDuration.Nanoseconds())/1000000.0,
				float64(actualDelay.Nanoseconds())/1000000.0))
			return
		}
		// Cancel failed, fall through to opposite order
	case <-time.After(50 * time.Millisecond): // Very short timeout
		// Cancel timeout, place opposite order
	}

	// Cancel failed or timed out, place opposite order
	tb.tradeConn.WriteMessage(websocket.TextMessage, closeMsg)

	select {
	case closeResp := <-closeRespChan:
		closeDuration := time.Since(startTime)
		if closeResp.RetCode == 0 {
			go sendMessage(chatId, fmt.Sprintf("🔄 Closed with opposite order!\n⚡ Close: %.3f ms\n🕐 Opened for: %.0f ms",
				float64(closeDuration.Nanoseconds())/1000000.0,
				float64(actualDelay.Nanoseconds())/1000000.0))
		} else {
			go sendMessage(chatId, fmt.Sprintf("❌ Close failed: %s", closeResp.RetMsg))
		}
	case <-time.After(200 * time.Millisecond):
		go sendMessage(chatId, "❌ Close timeout")
	}
}

// Ultra-optimized trade execution with minimal latency
func (tb *TradingBot) ExecuteTrade(req TradeRequest) {
	startTime := time.Now()

	// Check authentication status
	if err := tb.checkTradeConnection(); err != nil {
		sendMessage(req.ChatId, fmt.Sprintf("❌ %s", err.Error()))
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
	qtyStr := formatQuantity(qty)

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

	log.Printf("📤 Sending order: %s", string(orderMsg))

	// Setup response channel
	respChan := make(chan OrderResponse, 1)
	tb.responses.Store(reqId, respChan)
	defer tb.responses.Delete(reqId)

	// Pre-determine opposite side for closing
	oppositeSide := "Sell"
	if req.Side == "Sell" {
		oppositeSide = "Buy"
	}

	// Send order and record EXACT time
	orderOpenTime := time.Now()
	if err := tb.tradeConn.WriteMessage(websocket.TextMessage, orderMsg); err != nil {
		sendMessage(req.ChatId, fmt.Sprintf("❌ Failed to send order: %v", err))
		return
	}

	// Wait for response
	select {
	case resp := <-respChan:
		orderDuration := time.Since(orderOpenTime)

		if resp.RetCode != 0 {
			sendMessage(req.ChatId, fmt.Sprintf("❌ Order failed: %s\n🔍 Debug: Qty=%s (%.6f), Value=%.2f USDT, Price=%.8f",
				resp.RetMsg, qtyStr, parsedQty, positionValue, price))
			return
		}

		orderId := resp.Data.OrderId

		// Pre-build close messages immediately after getting orderId
		cancelReqId := generateReqId()
		closeReqId := generateReqId()
		cancelMsg := buildCancelMessage(cancelReqId, req.Symbol, orderId)
		closeMsg := buildOrderMessage(closeReqId, req.Symbol, oppositeSide, qtyStr)

		// Send success message asynchronously (don't wait)
		go func() {
			msg := fmt.Sprintf("🚀 ULTRA-FAST EXECUTION!\n"+
				"📊 %s %s %.2f USDT %.0fx\n"+
				"⚡ Open: %.3f ms\n"+
				"🆔 Order: %s\n"+
				"💰 Price: %.8f\n"+
				"📈 Qty: %s\n\n"+
				"⏳ Auto-closing in exactly 5000ms...",
				req.Symbol, req.Side, req.UsdtAmount, req.Leverage,
				float64(orderDuration.Nanoseconds())/1000000.0,
				orderId, price, qtyStr)
			sendMessage(req.ChatId, msg)
		}()

		// Calculate exact close time
		targetCloseTime := orderOpenTime.Add(AUTO_CLOSE_DELAY)

		// Use high-precision timer for exactly 5000ms
		timer := time.NewTimer(time.Until(targetCloseTime))

		go func() {
			<-timer.C
			closeStartTime := time.Now()
			actualDelay := closeStartTime.Sub(orderOpenTime)

			// Ultra-fast close execution
			tb.ultraFastClose(req.ChatId, req.Symbol, orderId, cancelReqId, closeReqId,
				cancelMsg, closeMsg, closeStartTime, actualDelay)
		}()

		// Report timing (asynchronously)
		go func() {
			totalDuration := time.Since(startTime)
			sendMessage(req.ChatId, fmt.Sprintf("⏱️ Total Setup: %.3f ms",
				float64(totalDuration.Nanoseconds())/1000000.0))
		}()

	case <-time.After(ORDER_TIMEOUT):
		sendMessage(req.ChatId, "❌ Order timeout")
		return
	}
}

// OpenPosition - opens a position and returns order details for custom closing logic
func (tb *TradingBot) OpenPosition(req TradeRequest) (string, error) {
	// Check authentication status
	if err := tb.checkTradeConnection(); err != nil {
		return "", fmt.Errorf("connection error: %s", err.Error())
	}

	// Get current price
	price, err := tb.GetPrice(req.Symbol)
	if err != nil {
		return "", fmt.Errorf("price not available for %s. Use /sub %s first", req.Symbol, req.Symbol)
	}

	// Calculate quantity with validation
	positionValue := req.UsdtAmount * req.Leverage
	qty := positionValue / price

	// Validate minimum order size
	if positionValue < 5.0 {
		return "", fmt.Errorf("minimum order value is 5 USDT (you specified %.2f USDT)", positionValue)
	}

	// Format quantity properly
	qtyStr := formatQuantity(qty)

	// Parse back to check if it's valid
	parsedQty, parseErr := strconv.ParseFloat(qtyStr, 64)
	if parseErr != nil || parsedQty <= 0 {
		return "", fmt.Errorf("invalid quantity format: %s", qtyStr)
	}

	// Additional validation for very small quantities
	if parsedQty < 0.1 {
		return "", fmt.Errorf("quantity too small: %s (minimum ~0.1)", qtyStr)
	}

	log.Printf("📊 Position opening: Symbol=%s, Price=%.8f, Value=%.2f, Qty=%s (%.6f)",
		req.Symbol, price, positionValue, qtyStr, parsedQty)

	// Create order
	reqId := generateReqId()
	orderMsg := buildOrderMessage(reqId, req.Symbol, req.Side, qtyStr)

	log.Printf("📤 Sending order: %s", string(orderMsg))

	// Setup response channel
	respChan := make(chan OrderResponse, 1)
	tb.responses.Store(reqId, respChan)
	defer tb.responses.Delete(reqId)

	// Send order
	if err := tb.tradeConn.WriteMessage(websocket.TextMessage, orderMsg); err != nil {
		return "", fmt.Errorf("failed to send order: %v", err)
	}

	// Wait for response
	select {
	case resp := <-respChan:
		if resp.RetCode != 0 {
			return "", fmt.Errorf("order failed: %s", resp.RetMsg)
		}
		return resp.Data.OrderId, nil

	case <-time.After(ORDER_TIMEOUT):
		return "", fmt.Errorf("order timeout")
	}
}

// Updated executeFundingStrategy - opens 1s before funding, closes at exact funding time
func (tb *TradingBot) executeFundingStrategy(strategy *FundingStrategy) {
	now := time.Now()
	fundingTime := strategy.NextFunding
	openTime := fundingTime.Add(-FUNDING_OPEN_DELAY) // 1 second before funding

	// Check if we missed the window
	if now.After(fundingTime) {
		sendMessage(strategy.ChatId, "❌ Missed funding window")
		return
	}

	if now.After(openTime) {
		sendMessage(strategy.ChatId, "❌ Missed opening window (too close to funding)")
		return
	}

	// Send notification about upcoming execution
	timeToOpen := time.Until(openTime)
	timeToFunding := time.Until(fundingTime)

	sendMessage(strategy.ChatId, fmt.Sprintf("🎯 FUNDING STRATEGY READY!\n"+
		"📊 %s %s %.2f USDT %.0fx\n"+
		"💰 Rate: %.6f%%\n"+
		"⏰ Opening in: %s\n"+
		"🎯 Funding in: %s\n"+
		"⚡ Will hold for exactly 1 second",
		strategy.Symbol, strategy.TargetSide, strategy.UsdtAmount, strategy.Leverage,
		strategy.FundingRate*100, formatDuration(timeToOpen), formatDuration(timeToFunding)))

	// Wait until exactly 1 second before funding
	log.Printf("🕐 Waiting until %s to open funding position...", openTime.Format("15:04:05.000"))
	time.Sleep(time.Until(openTime))

	// Open position with EXACT timing
	orderOpenTime := time.Now()
	req := TradeRequest{
		Symbol:     strategy.Symbol,
		Side:       strategy.TargetSide,
		UsdtAmount: strategy.UsdtAmount,
		Leverage:   strategy.Leverage,
		ChatId:     strategy.ChatId,
	}

	sendMessage(strategy.ChatId, fmt.Sprintf("🎯 OPENING FUNDING POSITION!\n"+
		"⏰ T-1s: Opening now...\n"+
		"🎯 Will close at exact funding time"))

	orderId, err := tb.OpenPosition(req)
	if err != nil {
		sendMessage(strategy.ChatId, fmt.Sprintf("❌ Failed to open funding position: %s", err.Error()))
		return
	}

	orderDuration := time.Since(orderOpenTime)

	// Send confirmation
	sendMessage(strategy.ChatId, fmt.Sprintf("✅ FUNDING POSITION OPENED!\n"+
		"⚡ Open Time: %.3f ms\n"+
		"🆔 Order ID: %s\n"+
		"⏰ Will hold for exactly 1000ms...",
		float64(orderDuration.Nanoseconds())/1000000.0, orderId))

	// Calculate exact close time (1000ms after open)
	targetCloseTime := orderOpenTime.Add(FUNDING_HOLD_DELAY)

	// Use high-precision timer for exactly 1000ms
	timer := time.NewTimer(time.Until(targetCloseTime))

	<-timer.C
	closeStartTime := time.Now()
	actualDelay := closeStartTime.Sub(orderOpenTime)

	// Close position with pre-built messages for ultra-fast execution
	oppositeSide := "Sell"
	if strategy.TargetSide == "Sell" {
		oppositeSide = "Buy"
	}

	qtyStr := formatQuantity(strategy.UsdtAmount * strategy.Leverage / tb.mustGetPrice(strategy.Symbol))

	// Pre-build close messages
	cancelReqId := generateReqId()
	closeReqId := generateReqId()
	cancelMsg := buildCancelMessage(cancelReqId, strategy.Symbol, orderId)
	closeMsg := buildOrderMessage(closeReqId, strategy.Symbol, oppositeSide, qtyStr)

	// Execute ultra-fast close
	tb.ultraFastClose(strategy.ChatId, strategy.Symbol, orderId, cancelReqId, closeReqId,
		cancelMsg, closeMsg, closeStartTime, actualDelay)
}

// Updated ExecuteTestTrade - holds position for exactly 1 second
func (tb *TradingBot) ExecuteTestTrade(req TradeRequest) {
	// Send initial notification
	sendMessage(req.ChatId, fmt.Sprintf("🧪 TEST TRADE STARTING!\n"+
		"📊 %s %s %.2f USDT %.0fx\n"+
		"⚡ Will hold for exactly 1 second...",
		req.Symbol, req.Side, req.UsdtAmount, req.Leverage))

	// Open position
	orderOpenTime := time.Now()
	orderId, err := tb.OpenPosition(req)
	if err != nil {
		sendMessage(req.ChatId, fmt.Sprintf("❌ Test trade failed: %s", err.Error()))
		return
	}

	orderDuration := time.Since(orderOpenTime)

	// Send success message
	sendMessage(req.ChatId, fmt.Sprintf("✅ TEST POSITION OPENED!\n"+
		"⚡ Open Time: %.3f ms\n"+
		"🆔 Order ID: %s\n"+
		"⏰ Holding for exactly 1 second...",
		float64(orderDuration.Nanoseconds())/1000000.0, orderId))

	// Calculate exact close time (1000ms after open)
	targetCloseTime := orderOpenTime.Add(FUNDING_HOLD_DELAY)

	// Use high-precision timer
	timer := time.NewTimer(time.Until(targetCloseTime))

	<-timer.C
	closeStartTime := time.Now()
	actualDelay := closeStartTime.Sub(orderOpenTime)

	// Close position with pre-built messages
	oppositeSide := "Sell"
	if req.Side == "Sell" {
		oppositeSide = "Buy"
	}

	price := tb.mustGetPrice(req.Symbol)
	qtyStr := formatQuantity(req.UsdtAmount * req.Leverage / price)

	// Pre-build close messages
	cancelReqId := generateReqId()
	closeReqId := generateReqId()
	cancelMsg := buildCancelMessage(cancelReqId, req.Symbol, orderId)
	closeMsg := buildOrderMessage(closeReqId, req.Symbol, oppositeSide, qtyStr)

	// Execute ultra-fast close
	tb.ultraFastClose(req.ChatId, req.Symbol, orderId, cancelReqId, closeReqId,
		cancelMsg, closeMsg, closeStartTime, actualDelay)
}

// Helper function to get price (must exist)
func (tb *TradingBot) mustGetPrice(symbol string) float64 {
	if price, exists := tb.priceCache.Load(symbol); exists {
		return price.(float64)
	}
	return 0 // This should not happen if validation was done properly
}

// Updated ExecuteFundingTrade - uses precise timing like funding strategy
func (tb *TradingBot) ExecuteFundingTrade(req TradeRequest) {
	startTime := time.Now()

	// Send initial notification
	sendMessage(req.ChatId, fmt.Sprintf("🎯 FUNDING-STYLE TRADE!\n"+
		"📊 %s %s %.2f USDT %.0fx\n"+
		"⚡ Will hold for exactly 1 second...",
		req.Symbol, req.Side, req.UsdtAmount, req.Leverage))

	// Open position
	orderOpenTime := time.Now()
	orderId, err := tb.OpenPosition(req)
	if err != nil {
		sendMessage(req.ChatId, fmt.Sprintf("❌ Funding trade failed: %s", err.Error()))
		return
	}

	orderDuration := time.Since(orderOpenTime)

	// Send success message
	sendMessage(req.ChatId, fmt.Sprintf("🎯 FUNDING POSITION OPENED!\n"+
		"⚡ Open Time: %.3f ms\n"+
		"🆔 Order ID: %s\n"+
		"⏰ Holding for exactly 1 second...",
		float64(orderDuration.Nanoseconds())/1000000.0, orderId))

	// Hold for exactly 1 second (simulating funding hold period)
	time.Sleep(1 * time.Second)

	// Close position
	sendMessage(req.ChatId, "🎯 Closing after funding-style hold!")

	oppositeSide := "Sell"
	if req.Side == "Sell" {
		oppositeSide = "Buy"
	}

	price := tb.mustGetPrice(req.Symbol)
	qtyStr := formatQuantity(req.UsdtAmount * req.Leverage / price)

	tb.immediateClose(req.ChatId, req.Symbol, orderId, oppositeSide, qtyStr,
		startTime, orderOpenTime, "FUNDING")
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
					"/sub <SYMBOL> - Subscribe to price & funding\n" +
					"/trade <SYMBOL> <BUY/SELL> <USDT> <LEVERAGE> - Execute trade\n" +
					"/funding <SYMBOL> - Show funding info\n" +
					"/strategy <SYMBOL> <USDT> <LEVERAGE> - Set funding strategy (auto-tests)\n" +
					"/test <SYMBOL> <USDT> <LEVERAGE> - Manual test trade\n" +
					"/strategies - List active strategies\n" +
					"/fundings - List all funding rates\n" +
					"/status - Connection status\n" +
					"/prices - Show cached prices\n" +
					"/debug <SYMBOL> - Debug symbol info\n\n" +
					"Examples:\n" +
					"/trade BTCUSDT BUY 10 2\n" +
					"/strategy BTCUSDT 10 2 (automatically tests!)"
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
					testQtyStr := formatQuantity(testQty) // FIXED: Removed symbol parameter

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

					// Remove the 2-second sleep that causes delay!
					// time.Sleep(2 * time.Second)

					// Check immediately if price data exists
					if price, err := tb.GetPrice(symbol); err == nil {
						sendMessage(chatId, fmt.Sprintf("✅ %s price confirmed: %.8f", symbol, price))
					} else {
						sendMessage(chatId, fmt.Sprintf("🔔 %s subscription sent. Price data will be available shortly.", symbol))
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

			case "funding":
				args := strings.Fields(text)
				if len(args) != 2 {
					sendMessage(chatId, "Usage: /funding <SYMBOL>\nExample: /funding BTCUSDT")
					continue
				}

				symbol := strings.ToUpper(args[1])
				if fundingInfo, exists := tb.fundingCache.Load(symbol); exists {
					info := fundingInfo.(FundingInfo)
					nextTime := time.Unix(info.FundingTimestamp, 0)
					timeLeft := time.Until(nextTime)

					msg := fmt.Sprintf("💰 %s Funding Info:\n"+
						"📊 Rate: %.6f%%\n"+
						"⏰ Next: %s\n"+
						"⏳ Time Left: %s",
						symbol, info.FundingRate*100,
						nextTime.Format("15:04:05 MST"),
						formatDuration(timeLeft))
					sendMessage(chatId, msg)
				} else {
					sendMessage(chatId, fmt.Sprintf("❌ No funding data for %s. Use /sub first.", symbol))
				}

			case "strategy":
				args := strings.Fields(text)
				if len(args) != 4 {
					sendMessage(chatId, "Usage: /strategy <SYMBOL> <USDT> <LEVERAGE>\n"+
						"Example: /strategy BTCUSDT 10 2")
					continue
				}

				symbol := strings.ToUpper(args[1])
				usdtAmount, err := strconv.ParseFloat(args[2], 64)
				if err != nil || usdtAmount <= 0 {
					sendMessage(chatId, "❌ Invalid USDT amount")
					continue
				}

				leverage, err := strconv.ParseFloat(args[3], 64)
				if err != nil || leverage <= 0 {
					sendMessage(chatId, "❌ Invalid leverage")
					continue
				}

				tb.setupFundingStrategy(chatId, symbol, usdtAmount, leverage)

			case "strategies":
				var stratList []string
				tb.strategies.Range(func(key, value interface{}) bool {
					symbol := key.(string)
					strategy := value.(*FundingStrategy)
					timeLeft := time.Until(strategy.NextFunding)
					stratList = append(stratList, fmt.Sprintf(
						"%s: %.2f USDT %.0fx %s (%.6f%%) in %s",
						symbol, strategy.UsdtAmount, strategy.Leverage,
						strategy.TargetSide, strategy.FundingRate*100,
						formatDuration(timeLeft)))
					return true
				})

				if len(stratList) == 0 {
					sendMessage(chatId, "📊 No active funding strategies")
				} else {
					msg := "📊 Active Funding Strategies:\n" + strings.Join(stratList, "\n")
					sendMessage(chatId, msg)
				}

			case "fundings":
				var fundingList []string
				tb.fundingCache.Range(func(key, value interface{}) bool {
					symbol := key.(string)
					info := value.(FundingInfo)
					nextTime := time.Unix(info.FundingTimestamp, 0)
					timeLeft := time.Until(nextTime)

					fundingList = append(fundingList, fmt.Sprintf(
						"%s: %.6f%% in %s",
						symbol, info.FundingRate*100, formatDuration(timeLeft)))
					return true
				})

				if len(fundingList) == 0 {
					sendMessage(chatId, "📊 No funding data cached yet")
				} else {
					msg := "💰 Cached Funding Rates:\n" + strings.Join(fundingList, "\n")
					sendMessage(chatId, msg)
				}

			case "auth":
				if atomic.LoadInt32(&tb.connReady) == 1 {
					sendMessage(chatId, "✅ Trade connection authenticated")
				} else {
					sendMessage(chatId, "❌ Trade connection not authenticated")

					// Show current auth details for debugging
					timestamp := time.Now().UnixMilli()
					expiryTimestamp := timestamp + 10000
					signature := generateSignature(BYBIT_API_SECRET, expiryTimestamp)

					debugMsg := fmt.Sprintf("🔐 Debug Auth Info:\n"+
						"API Key: %s\n"+
						"Current Time: %d\n"+
						"Expiry Time: %d\n"+
						"Signature: %s\n"+
						"Payload: GET/realtime%d",
						BYBIT_API_KEY, timestamp, expiryTimestamp, signature, expiryTimestamp)

					sendMessage(chatId, debugMsg)

					// Try to re-authenticate
					authMsg := buildAuthMessage(BYBIT_API_KEY, BYBIT_API_SECRET)
					log.Printf("🔐 Re-sending auth message: %s", string(authMsg))

					if err := tb.tradeConn.WriteMessage(websocket.TextMessage, authMsg); err != nil {
						sendMessage(chatId, fmt.Sprintf("❌ Failed to re-authenticate: %v", err))
					} else {
						sendMessage(chatId, "🔄 Re-authentication sent, waiting for response...")
					}
				}
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
		log.Fatal("Failed to connect trading bot:", err)
	}

	// Start handling Telegram updates
	handleUpdates(tb)
}

// SubscribeToSymbol subscribes to ticker updates for a symbol
func (tb *TradingBot) SubscribeToSymbol(symbol string) error {
	tickerMsg := buildSubscribeMessage(symbol)

	if err := tb.publicConn.WriteMessage(websocket.TextMessage, tickerMsg); err != nil {
		return fmt.Errorf("failed to subscribe to ticker: %v", err)
	}

	// Wait for initial data - just a short wait for the first message
	time.Sleep(500 * time.Millisecond)

	// Check if price data exists
	_, priceExists := tb.priceCache.Load(symbol)

	if priceExists {
		log.Printf("✅ %s subscription confirmed with price data", symbol)
	} else {
		log.Printf("⏳ %s subscription active, waiting for first ticker message...", symbol)
	}

	return nil
}

// updateFundingStrategy updates an existing strategy with new funding info
func (tb *TradingBot) updateFundingStrategy(strategy *FundingStrategy, info FundingInfo) {
	// Update strategy with new funding information
	nextFunding := time.Unix(info.FundingTimestamp, 0)

	// Only update if the new funding time is different and in the future
	if !strategy.NextFunding.Equal(nextFunding) && nextFunding.After(time.Now()) {
		strategy.NextFunding = nextFunding
		strategy.FundingRate = info.FundingRate

		// Update target side based on new funding rate
		targetSide := "Buy"
		if info.FundingRate > 0 {
			targetSide = "Sell" // Shorts get paid when rate is positive
		}
		strategy.TargetSide = targetSide

		// Update the strategy in the map
		tb.strategies.Store(strategy.Symbol, strategy)

		log.Printf("🔄 Updated strategy for %s: Rate=%.6f%%, Side=%s, Next=%s",
			strategy.Symbol, info.FundingRate*100, targetSide,
			nextFunding.Format("15:04:05 MST"))

		// Send update to user
		timeLeft := time.Until(nextFunding)
		msg := fmt.Sprintf("🔄 STRATEGY UPDATED!\n"+
			"📊 %s %s %.2f USDT %.0fx\n"+
			"💰 New Rate: %.6f%%\n"+
			"⏰ Execute in: %s",
			strategy.Symbol, targetSide, strategy.UsdtAmount, strategy.Leverage,
			info.FundingRate*100, formatDuration(timeLeft))

		sendMessage(strategy.ChatId, msg)
	}
}

// Add a function to check connection status before trading
func (tb *TradingBot) checkTradeConnection() error {
	if atomic.LoadInt32(&tb.connReady) != 1 {
		return fmt.Errorf("trade connection not authenticated")
	}
	return nil
}

// Add these methods after the TradingBot struct definition

// GetPrice returns the cached price for a symbol
func (tb *TradingBot) GetPrice(symbol string) (float64, error) {
	if price, exists := tb.priceCache.Load(symbol); exists {
		return price.(float64), nil
	}
	return 0, fmt.Errorf("no price data for %s", symbol)
}

// immediateClose attempts to close a position as fast as possible
func (tb *TradingBot) immediateClose(chatId int64, symbol, orderId, side, qty string,
	startTime, openTime time.Time, tradeType string) {

	// Create request IDs
	cancelReqId := generateReqId()
	closeReqId := generateReqId()

	// Build messages
	cancelMsg := buildCancelMessage(cancelReqId, symbol, orderId)
	closeMsg := buildOrderMessage(closeReqId, symbol, side, qty)

	// Execute ultra-fast close
	tb.ultraFastClose(chatId, symbol, orderId, cancelReqId, closeReqId,
		cancelMsg, closeMsg, startTime, time.Since(openTime))
}

// setupFundingStrategy creates or updates a funding strategy for a symbol
func (tb *TradingBot) setupFundingStrategy(chatId int64, symbol string, usdtAmount, leverage float64) {
	// Get funding info
	fundingInfo, exists := tb.fundingCache.Load(symbol)
	if !exists {
		sendMessage(chatId, fmt.Sprintf("❌ No funding data for %s. Use /sub first.", symbol))
		return
	}

	info := fundingInfo.(FundingInfo)
	nextFunding := time.Unix(info.FundingTimestamp, 0)

	// Determine trade side based on funding rate
	targetSide := "Buy"
	if info.FundingRate > 0 {
		targetSide = "Sell" // Shorts get paid when rate is positive
	}

	// Create strategy
	strategy := &FundingStrategy{
		Symbol:      symbol,
		UsdtAmount:  usdtAmount,
		Leverage:    leverage,
		ChatId:      chatId,
		NextFunding: nextFunding,
		FundingRate: info.FundingRate,
		TargetSide:  targetSide,
	}

	// Store strategy
	tb.strategies.Store(symbol, strategy)

	// Send confirmation
	timeLeft := time.Until(nextFunding)
	msg := fmt.Sprintf("✅ FUNDING STRATEGY SET!\n"+
		"📊 %s %s %.2f USDT %.0fx\n"+
		"💰 Rate: %.6f%%\n"+
		"⏰ Execute in: %s",
		symbol, targetSide, usdtAmount, leverage,
		info.FundingRate*100, formatDuration(timeLeft))
	sendMessage(chatId, msg)

	// First run a test trade with the same parameters
	sendMessage(chatId, "🧪 Running test trade before scheduling funding trade...")
	testReq := TradeRequest{
		Symbol:     symbol,
		Side:       targetSide,
		UsdtAmount: usdtAmount,
		Leverage:   leverage,
		ChatId:     chatId,
	}

	// Execute test trade synchronously
	tb.ExecuteTestTrade(testReq)

	// Wait a bit after test trade
	time.Sleep(2 * time.Second)

	// If still enough time until funding, start the strategy
	if time.Until(nextFunding) > 2*time.Minute {
		sendMessage(chatId, "✅ Test trade completed. Starting funding strategy...")
		go tb.executeFundingStrategy(strategy)
	} else {
		sendMessage(chatId, "❌ Too close to funding time after test. Will try next funding time.")
	}
}

// buildOrderMessage creates an order message with proper timestamp handling
func buildOrderMessage(reqId, symbol, side, qty string) []byte {
	// Add a small buffer to account for network latency and clock drift
	timestamp := strconv.FormatInt(time.Now().UnixMilli()+1000, 10) // Add 1 second buffer

	orderJSON := fmt.Sprintf(`{"reqId":"%s","header":{"X-BAPI-TIMESTAMP":"%s","X-BAPI-RECV-WINDOW":"10000"},"op":"order.create","args":[{"symbol":"%s","side":"%s","orderType":"Market","qty":"%s","category":"linear","timeInForce":"IOC","reduceOnly":false}]}`,
		reqId, timestamp, symbol, side, qty)

	return []byte(orderJSON)
}

// buildCancelMessage creates a cancel order message with proper timestamp handling
func buildCancelMessage(reqId, symbol, orderId string) []byte {
	// Add a small buffer to account for network latency and clock drift
	timestamp := strconv.FormatInt(time.Now().UnixMilli()+1000, 10) // Add 1 second buffer

	cancelJSON := fmt.Sprintf(`{"reqId":"%s","header":{"X-BAPI-TIMESTAMP":"%s","X-BAPI-RECV-WINDOW":"10000"},"op":"order.cancel","args":[{"symbol":"%s","orderId":"%s","category":"linear"}]}`,
		reqId, timestamp, symbol, orderId)

	return []byte(cancelJSON)
}

// buildSubscribeMessage creates a subscription message using pre-compiled templates
func buildSubscribeMessage(symbol string) []byte {
	// Use the pre-compiled templates for zero-allocation
	var buf bytes.Buffer
	buf.Write(subPrefix)
	buf.WriteString(symbol)
	buf.Write(subSuffix)

	return buf.Bytes()
}

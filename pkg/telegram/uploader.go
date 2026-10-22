package telegram

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/gotd/td/session"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/message"
	"github.com/gotd/td/telegram/uploader"
	"github.com/gotd/td/tg"
)

// Config holds Telegram connection credentials.
type Config struct {
	AppID            int
	AppHash          string
	BotToken         string
	SessionFile      string
	TargetPeer       string // e.g. "@my_private_channel" or empty for Saved Messages
	UploadPartSizeKB int
	UploadThreads    int
}

// URLHandler is called when a URL is sent to the bot.
type URLHandler func(ctx context.Context, targetURL string, reply func(text string))

// Client wraps gotd/td Telegram MTProto client.
type Client struct {
	cfg        Config
	client     *telegram.Client
	rawAPI     *tg.Client
	sender     *message.Sender
	up         *uploader.Uploader
	dispatcher tg.UpdateDispatcher
	onURL      URLHandler
}

// SetURLHandler registers a handler for URLs received in Telegram chat.
func (c *Client) SetURLHandler(h URLHandler) {
	c.onURL = h
}

// NewClient initializes a Telegram client.
func NewClient(cfg Config) (*Client, error) {
	if cfg.SessionFile == "" {
		cfg.SessionFile = "tg_session.json"
	}

	dispatcher := tg.NewUpdateDispatcher()

	opts := telegram.Options{
		SessionStorage: &session.FileStorage{
			Path: cfg.SessionFile,
		},
		UpdateHandler: dispatcher,
	}

	client := telegram.NewClient(cfg.AppID, cfg.AppHash, opts)

	c := &Client{
		cfg:        cfg,
		client:     client,
		dispatcher: dispatcher,
	}

	return c, nil
}

// Start connects to Telegram, authenticates, and invokes the worker function.
func (c *Client) Start(ctx context.Context, f func(ctx context.Context) error) error {
	return c.client.Run(ctx, func(ctx context.Context) error {
		// Authenticate with BotToken if provided
		if c.cfg.BotToken != "" {
			status, err := c.client.Auth().Status(ctx)
			if err != nil {
				return fmt.Errorf("failed to get auth status: %w", err)
			}
			if !status.Authorized {
				if _, err := c.client.Auth().Bot(ctx, c.cfg.BotToken); err != nil {
					return fmt.Errorf("bot authentication failed: %w", err)
				}
			}
		}

		c.rawAPI = c.client.API()
		c.sender = message.NewSender(c.rawAPI)

		partSize := c.cfg.UploadPartSizeKB * 1024
		if partSize <= 0 {
			partSize = 512 * 1024
		}
		threads := c.cfg.UploadThreads
		if threads <= 0 {
			threads = 4
		}
		c.up = uploader.NewUploader(c.rawAPI).WithPartSize(partSize).WithThreads(threads)

		// Register message handlers
		c.dispatcher.OnNewMessage(func(ctx context.Context, e tg.Entities, u *tg.UpdateNewMessage) error {
			if msg, ok := u.Message.(*tg.Message); ok && !msg.Out {
				c.handleMessage(ctx, e, u, msg.Message)
			}
			return nil
		})

		c.dispatcher.OnNewChannelMessage(func(ctx context.Context, e tg.Entities, u *tg.UpdateNewChannelMessage) error {
			if msg, ok := u.Message.(*tg.Message); ok && !msg.Out {
				c.handleMessage(ctx, e, u, msg.Message)
			}
			return nil
		})

		return f(ctx)
	})
}

func (c *Client) handleMessage(ctx context.Context, e tg.Entities, u message.AnswerableMessageUpdate, text string) {
	text = strings.TrimSpace(text)
	if text == "" {
		return
	}

	replyFunc := func(replyText string) {
		if c.sender != nil {
			_, _ = c.sender.Answer(e, u).Text(ctx, replyText)
		}
	}

	if strings.HasPrefix(text, "/start") {
		replyFunc("👋 Привет! Я бот для быстрой загрузки больших файлов.\n\nПросто отправь мне прямую ссылку (начинается с http:// или https://) или команду:\n/dl <ссылка>\n\nЯ скачаю файл чанками на VPS и выгружу в ваш канал!")
		return
	}

	var targetURL string
	if strings.HasPrefix(text, "/dl ") || strings.HasPrefix(text, "/download ") {
		fields := strings.Fields(text)
		if len(fields) >= 2 {
			targetURL = fields[1]
		}
	} else if strings.HasPrefix(text, "http://") || strings.HasPrefix(text, "https://") {
		fields := strings.Fields(text)
		if len(fields) > 0 {
			targetURL = fields[0]
		}
	}

	if targetURL != "" && c.onURL != nil {
		c.onURL(ctx, targetURL, replyFunc)
	}
}

// UploadChunk uploads a chunk file to Telegram and returns the sent message ID.
func (c *Client) UploadChunk(ctx context.Context, filePath, filename, caption string) (int, error) {
	if _, err := os.Stat(filePath); err != nil {
		return 0, fmt.Errorf("chunk file not found: %w", err)
	}

	upload, err := c.up.FromPath(ctx, filePath)
	if err != nil {
		return 0, fmt.Errorf("failed to upload chunk to MTProto: %w", err)
	}

	target, err := c.getTargetBuilder(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to resolve target peer: %w", err)
	}

	doc := message.UploadedDocument(upload).
		Filename(filepath.Base(filename)).
		MIME("application/octet-stream")

	updates, err := target.Media(ctx, doc)
	if err != nil {
		return 0, fmt.Errorf("failed to send media message: %w", err)
	}

	return extractMsgID(updates), nil
}

// SendTextMessage sends a plain text message (e.g. Manifest or notification).
func (c *Client) SendTextMessage(ctx context.Context, text string) (int, error) {
	target, err := c.getTargetBuilder(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to resolve target peer: %w", err)
	}

	updates, err := target.Text(ctx, text)
	if err != nil {
		return 0, fmt.Errorf("failed to send text message: %w", err)
	}

	return extractMsgID(updates), nil
}

func (c *Client) getTargetBuilder(ctx context.Context) (*message.RequestBuilder, error) {
	peerStr := strings.TrimSpace(c.cfg.TargetPeer)
	if peerStr == "" || strings.EqualFold(peerStr, "self") {
		return c.sender.Self(), nil
	}

	// If numeric ID (e.g. -1003752790574 or 3752790574)
	cleanID := strings.TrimPrefix(peerStr, "-100")
	cleanID = strings.TrimPrefix(cleanID, "-")
	if channelID, err := strconv.ParseInt(cleanID, 10, 64); err == nil {
		// 1. Resolve AccessHash via UpdatesGetDifference
		state, err := c.rawAPI.UpdatesGetState(ctx)
		if err == nil {
			diff, err := c.rawAPI.UpdatesGetDifference(ctx, &tg.UpdatesGetDifferenceRequest{
				Pts:  1,
				Date: state.Date - 86400,
				Qts:  0,
			})
			if err == nil {
				var chats []tg.ChatClass
				switch d := diff.(type) {
				case *tg.UpdatesDifference:
					chats = d.Chats
				case *tg.UpdatesDifferenceSlice:
					chats = d.Chats
				}
				for _, chat := range chats {
					if ch, ok := chat.(*tg.Channel); ok && ch.ID == channelID {
						return c.sender.To(&tg.InputPeerChannel{
							ChannelID:  ch.ID,
							AccessHash: ch.AccessHash,
						}), nil
					}
				}
			}
		}

		// 2. Fallback check via MessagesGetChats
		chats, err := c.rawAPI.MessagesGetChats(ctx, []int64{channelID})
		if err == nil {
			for _, chat := range chats.GetChats() {
				if ch, ok := chat.(*tg.Channel); ok && ch.ID == channelID {
					return c.sender.To(&tg.InputPeerChannel{
						ChannelID:  ch.ID,
						AccessHash: ch.AccessHash,
					}), nil
				}
			}
		}
	}

	return c.sender.Resolve(peerStr), nil
}

func extractMsgID(updates tg.UpdatesClass) int {
	switch u := updates.(type) {
	case *tg.UpdateShortSentMessage:
		return u.ID
	case *tg.UpdateShortMessage:
		return u.ID
	case *tg.UpdateShortChatMessage:
		return u.ID
	case *tg.Updates:
		for _, upd := range u.Updates {
			switch m := upd.(type) {
			case *tg.UpdateNewMessage:
				if msg, ok := m.Message.(*tg.Message); ok {
					return msg.ID
				}
			case *tg.UpdateNewChannelMessage:
				if msg, ok := m.Message.(*tg.Message); ok {
					return msg.ID
				}
			}
		}
	case *tg.UpdatesCombined:
		for _, upd := range u.Updates {
			switch m := upd.(type) {
			case *tg.UpdateNewMessage:
				if msg, ok := m.Message.(*tg.Message); ok {
					return msg.ID
				}
			case *tg.UpdateNewChannelMessage:
				if msg, ok := m.Message.(*tg.Message); ok {
					return msg.ID
				}
			}
		}
	}
	return 0
}

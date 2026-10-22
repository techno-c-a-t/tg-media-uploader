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
	AppID       int
	AppHash     string
	BotToken    string
	SessionFile string
	TargetPeer  string // e.g. "@my_private_channel" or empty for Saved Messages
}

// Client wraps gotd/td Telegram MTProto client.
type Client struct {
	cfg    Config
	client *telegram.Client
	rawAPI *tg.Client
	sender *message.Sender
	up     *uploader.Uploader
}

// NewClient initializes a Telegram client.
func NewClient(cfg Config) (*Client, error) {
	if cfg.SessionFile == "" {
		cfg.SessionFile = "tg_session.json"
	}

	opts := telegram.Options{
		SessionStorage: &session.FileStorage{
			Path: cfg.SessionFile,
		},
	}

	client := telegram.NewClient(cfg.AppID, cfg.AppHash, opts)

	return &Client{
		cfg:    cfg,
		client: client,
	}, nil
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
		c.up = uploader.NewUploader(c.rawAPI).WithPartSize(512 * 1024) // 512 KB chunks for MTProto upload

		return f(ctx)
	})
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

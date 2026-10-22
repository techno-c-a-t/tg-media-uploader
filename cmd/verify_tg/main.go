package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"tg_file_proxy/config"

	"github.com/gotd/td/session"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/message"
	"github.com/gotd/td/tg"
)

func main() {
	cfg := config.LoadFromEnv()

	opts := telegram.Options{
		SessionStorage: &session.FileStorage{
			Path: "test_session.json",
		},
	}
	client := telegram.NewClient(cfg.AppID, cfg.AppHash, opts)

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	_ = client.Run(ctx, func(clientCtx context.Context) error {
		auth, err := client.Auth().Bot(clientCtx, cfg.BotToken)
		if err != nil {
			return err
		}
		user, _ := auth.User.AsNotEmpty()
		log.Printf("✅ Bot: @%s (ID: %d)", user.Username, user.ID)

		rawAPI := client.API()
		sender := message.NewSender(rawAPI)

		// Fetch updates to get the channel's AccessHash!
		state, err := rawAPI.UpdatesGetState(clientCtx)
		if err != nil {
			log.Printf("UpdatesGetState error: %v", err)
			return err
		}

		diff, err := rawAPI.UpdatesGetDifference(clientCtx, &tg.UpdatesGetDifferenceRequest{
			Pts:  1,
			Date: state.Date - 3600,
			Qts:  0,
		})

		var channelPeer *tg.InputPeerChannel
		if err == nil {
			var chats []tg.ChatClass
			switch d := diff.(type) {
			case *tg.UpdatesDifference:
				chats = d.Chats
			case *tg.UpdatesDifferenceSlice:
				chats = d.Chats
			}

			for _, c := range chats {
				if ch, ok := c.(*tg.Channel); ok {
					log.Printf("🎉 Found Channel via Updates: '%s' (ID: %d, AccessHash: %d)", ch.Title, ch.ID, ch.AccessHash)
					channelPeer = &tg.InputPeerChannel{
						ChannelID:  ch.ID,
						AccessHash: ch.AccessHash,
					}
					break
				}
			}
		}

		if channelPeer == nil {
			log.Fatal("Could not find channel in updates")
		}

		log.Printf("Sending confirmation message to channel ID: %d...", channelPeer.ChannelID)
		target := sender.To(channelPeer)
		_, err = target.Text(clientCtx, fmt.Sprintf("🚀 Ура! Бот успешно подключен к закрытому каналу Mediaproxy!\n🕒 Время: %s\nГотов к приему файлов до 2 ГБ.", time.Now().Format("15:04:05")))
		if err != nil {
			log.Fatalf("Failed to send message: %v", err)
		}

		log.Println("🎉 SUCCESS: Message appeared in channel Mediaproxy!")
		return nil
	})
}

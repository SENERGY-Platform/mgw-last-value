/*
 * Copyright 2021 InfAI (CC SES)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package mqtt

import (
	"context"
	"log"
	"log/slog"
	"sync"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
)

func New(ctx context.Context, brokerUrl string, clientId string, username string, password string) (client *Mqtt, err error) {
	client = &Mqtt{
		subscriptions:    map[string]paho.MessageHandler{},
		subscriptionsMux: sync.Mutex{},
		mqtt:             nil,
		brokerUrl:        brokerUrl,
		clientId:         clientId,
		username:         username,
		password:         password,
	}
	return client, client.init(ctx)
}

type Mqtt struct {
	subscriptions    map[string]paho.MessageHandler
	subscriptionsMux sync.Mutex
	mqtt             paho.Client
	brokerUrl        string
	clientId         string
	username         string
	password         string
}

func (this *Mqtt) init(ctx context.Context) error {
	options := paho.NewClientOptions().
		SetPassword(this.password).
		SetUsername(this.username).
		SetAutoReconnect(true).
		SetCleanSession(true).
		SetClientID(this.clientId).
		AddBroker(this.brokerUrl).
		SetResumeSubs(true).
		SetWriteTimeout(10 * time.Second).
		SetOrderMatters(false).
		SetConnectionLostHandler(func(_ paho.Client, err error) {
			slog.Error("connection to mqtt broker lost", "error", err)
		}).
		SetOnConnectHandler(func(_ paho.Client) {
			slog.Info("(re)connected to mqtt broker")
			err := this.loadOldSubscriptions()
			if err != nil {
				slog.Error("FATAL: unable to load old subscriptions", "error", err)
				log.Fatal("FATAL: ", err)
			}
		})

	this.mqtt = paho.NewClient(options)
	if token := this.mqtt.Connect(); token.Wait() && token.Error() != nil {
		slog.Error("unable to connect to mqtt broker", "error", token.Error())
		return token.Error()
	}

	go func() {
		<-ctx.Done()
		this.mqtt.Disconnect(0)
	}()
	return nil
}

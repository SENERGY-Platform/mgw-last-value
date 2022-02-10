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
	paho "github.com/eclipse/paho.mqtt.golang"
	"log"
)

func (this *Mqtt) Subscribe(topic string, qos byte, handler func(topic string, payload []byte)) error {
	f := func(client paho.Client, message paho.Message) {
		handler(message.Topic(), message.Payload())
	}
	token := this.mqtt.Subscribe(topic, qos, f)
	if token.Wait() && token.Error() != nil {
		log.Println("Error on Subscribe: ", topic, token.Error())
		return token.Error()
	}
	this.registerSubscription(topic, f)
	return nil
}

func (this *Mqtt) Unsubscribe(topic string) error {
	token := this.mqtt.Unsubscribe(topic)
	if token.Wait() && token.Error() != nil {
		log.Println("Error on Subscribe: ", topic, token.Error())
		return token.Error()
	}
	this.unregisterSubscriptions(topic)
	return nil
}

func (this *Mqtt) Publish(topic string, qos byte, retained bool, payload []byte) error {
	token := this.mqtt.Publish(topic, qos, retained, payload)
	if token.Wait() && token.Error() != nil {
		log.Println("Error on Mqtt.Publish(): ", token.Error())
		return token.Error()
	}
	return nil
}

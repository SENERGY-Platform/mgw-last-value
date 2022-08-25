/*
 * Copyright 2022 InfAI (CC SES)
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

package bolt

import (
	"context"
	"encoding/json"
	"github.com/SENERGY-Platform/mgw-last-value/pkg/configuration"
	"go.etcd.io/bbolt"
	"log"
	"sync"
	"time"
)

var BBOLT_BUCKET_NAME = []byte("last_value")

type Store struct {
	db *bbolt.DB
}

func NewWithConfig(ctx context.Context, wg *sync.WaitGroup, config configuration.Config) (result *Store, err error) {
	return New(ctx, wg, config.BoltLocation)
}

func New(ctx context.Context, wg *sync.WaitGroup, location string) (result *Store, err error) {
	log.Println("start bolt")
	result = &Store{}
	result.db, err = bbolt.Open(location, 0666, nil)
	if err != nil {
		return result, err
	}

	err = result.db.Update(func(tx *bbolt.Tx) error {
		_, err = tx.CreateBucketIfNotExists(BBOLT_BUCKET_NAME)
		return err
	})
	if err != nil {
		result.db.Close()
		return result, err
	}

	if wg != nil {
		wg.Add(1)
	}
	go func() {
		if wg != nil {
			defer wg.Done()
		}
		<-ctx.Done()
		err = result.db.Close()
		if err != nil {
			log.Println("WARNING: unable to close bolt file:", err)
		}
	}()

	return result, nil
}

type ValueWithTime struct {
	Value []byte    `json:"v"`
	Time  time.Time `json:"t"`
}

func (this *Store) Set(key string, value []byte) error {
	jsonValue, err := json.Marshal(ValueWithTime{Value: value, Time: time.Now()})
	if err != nil {
		return err
	}
	return this.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(BBOLT_BUCKET_NAME).Put([]byte(key), jsonValue)
	})
}

func (this *Store) Get(key string) (value []byte, time *time.Time, err error) {
	err = this.db.View(func(tx *bbolt.Tx) error {
		temp := tx.Bucket(BBOLT_BUCKET_NAME).Get([]byte(key))
		if temp == nil {
			value = []byte("null")
			return nil
		}
		valWithTime := ValueWithTime{}
		err = json.Unmarshal(temp, &valWithTime)
		if err != nil {
			log.Println("ERROR: unable to unmarshal value from bolt", err)
			return err
		}
		value = valWithTime.Value
		time = &valWithTime.Time
		return nil
	})
	return value, time, err
}

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

package badger

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/SENERGY-Platform/mgw-last-value/pkg/configuration"
	"github.com/dgraph-io/badger/v3"
	"log"
	"sync"
	"time"
)

type BadgerStore struct {
	db  *badger.DB
	ttl time.Duration
}

func NewWithConfig(ctx context.Context, wg *sync.WaitGroup, config configuration.Config) (result *BadgerStore, err error) {
	return New(ctx, wg, config.BadgerLocation, config.BadgerGcInterval, config.BadgerTtl)
}

func New(ctx context.Context, wg *sync.WaitGroup, location string, intervalStr string, ttlDurationString string) (result *BadgerStore, err error) {
	var ttl time.Duration
	if ttlDurationString != "" {
		ttl, err = time.ParseDuration(ttlDurationString)
		if err != nil {
			return result, errors.New("unable to parse badger ttl as duration:" + err.Error())
		}
	}

	interval, err := time.ParseDuration(intervalStr)
	if err != nil {
		return result, errors.New("unable to parse badger garbage collection interval as duration:" + err.Error())
	}
	result = &BadgerStore{
		ttl: ttl,
	}
	result.db, err = badger.Open(badger.DefaultOptions(location))
	if err != nil {
		return result, err
	}

	//implement stop cleanup
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
			log.Println("WARNING: unable to close badger connection:", err)
		}
	}()

	//implement garbage collection
	if wg != nil {
		wg.Add(1)
	}
	go func() {
		if wg != nil {
			defer wg.Done()
		}
		ticker := time.NewTicker(interval)
		for {
			select {
			case <-ctx.Done():
				ticker.Stop()
				return
			case <-ticker.C:
				var err error
				for err == nil {
					err = result.db.RunValueLogGC(0.5)
				}
			}
		}
	}()

	return result, nil
}

type ValueWithTime struct {
	Value []byte    `json:"v"`
	Time  time.Time `json:"t"`
}

func (this *BadgerStore) Set(key string, value []byte) error {
	jsonValue, err := json.Marshal(ValueWithTime{Value: value, Time: time.Now()})
	if err != nil {
		return err
	}
	return this.db.Update(func(txn *badger.Txn) error {
		entry := badger.NewEntry([]byte(key), jsonValue)
		if this.ttl != 0 {
			entry.WithTTL(this.ttl)
		}
		return txn.SetEntry(entry)
	})
}

func (this *BadgerStore) Get(key string) (value []byte, time *time.Time, err error) {
	valWithTime := ValueWithTime{}
	err = this.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err == badger.ErrKeyNotFound {
			//not found --> value = nil
			value = []byte("null")
			return nil
		}
		if err != nil {
			log.Println("ERROR: unable to read value from badger", err)
			return err
		}
		temp, err := item.ValueCopy(nil)
		if err != nil {
			log.Println("ERROR: unable to copy value from badger", err)
			return err
		}
		err = json.Unmarshal(temp, &valWithTime)
		if err != nil {
			log.Println("ERROR: unable to unmarshal value from badger", err)
			return err
		}
		value = valWithTime.Value
		time = &valWithTime.Time
		return nil
	})
	return value, time, err
}

package main

import (
	"fmt"
	"log"
	"time"

	"github.com/garyburd/redigo/redis"
)

const (
	apnsJSONTemplate = `{"token":"99999999999999999999999999%5d999999999999999999999999999999999","badge":1,content_available:0,"body":"Your Timehop day is ready!\nView it now →"}`
)

func main() {
	r, err := redis.Dial("tcp", ":6379")
	if err != nil {
		log.Fatal("couldn't connect to redis")
	}
	defer r.Close()

	r.Do("SELECT", "5")

	msgs := make([]string, 33333)
	for i := 0; i < 33333; i++ {
		msgs[i] = fmt.Sprintf(apnsJSONTemplate, i)
	}

	tz, _ := time.LoadLocation("America/New_York")
	t := time.Date(2014, 1, 17, 4, 0, 0, 0, tz)

	tAdd := time.Now()
	for hr := 0; hr < 4; hr++ {
		for min := 15; min <= 45; min++ {
			nt := t.Add(time.Duration(hr) * time.Hour).Add(time.Duration(min) * time.Minute)
			fmt.Println("Adding for", nt)
			for _, s := range msgs {
				r.Do("ZADD", "apns:push:delayed", fmt.Sprint(nt.Unix()), s)
			}
		}
	}
	fmt.Println("Adding", time.Since(tAdd))

	tPop := time.Now()
	for hr := 0; hr < 4; hr++ {
		for min := 15; min <= 45; min++ {
			nt := t.Add(time.Duration(hr) * time.Hour).Add(time.Duration(min) * time.Minute)
			fmt.Println("Popping for", nt)
			for {
				m, _ := redis.Strings(r.Do("zrangebyscore", "apns:push:delayed", "-inf", fmt.Sprint(nt.Unix()), "limit", 0, 1))

				if len(m) == 0 {
					break
				}

				redis.Bool(r.Do("zrem", "apns:push:delayed", m[0]))
			}
		}
	}
	fmt.Println("Popping", time.Since(tPop))
}

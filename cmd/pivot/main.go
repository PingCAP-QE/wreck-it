package main

import (
	"context"
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/zhouqiang-cl/wreck-it/pkg/pivot"
	"time"
)

var (
	dsn string
)

func main() {
	// p.Start(context.Background())
	flag.StringVar(&dsn, "d", "", "dsn of target db for testing")
	flag.Parse()
	if dsn == "" {
		panic("no dsn in arguments")
	}

	p, err := pivot.NewPivot(dsn, "test")
	if err != nil {
		panic(fmt.Sprintf("new pivot failed, error: %+v\n", err))
	}

	//p.Conf.Dsn = "root@tcp(127.0.0.1:4000)/test"
	ctx, _ := context.WithTimeout(context.Background(), 20 * time.Second)
	fmt.Printf("start pivot test\n")
	p.Start(ctx)
	fmt.Printf("wait for finish\n")
	for {
		select {
		case <- ctx.Done():
			p.Close()
		default:
		}
	}

}

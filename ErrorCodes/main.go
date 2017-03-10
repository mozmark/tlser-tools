package main

import (
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"time"
	
	"github.com/gogo/protobuf/proto"
	"github.com/mozilla-services/heka/message"
	"github.com/mozilla-services/heka/pipeline"
)

func makeSplitterRunner() (pipeline.SplitterRunner, error) {
	splitter := &pipeline.HekaFramingSplitter{}
	config := splitter.ConfigStruct()
	err := splitter.Init(config)
	if err != nil {
		return nil, fmt.Errorf("Error initializing HekaFramingSplitter: %s", err)
	}
	srConfig := pipeline.CommonSplitterConfig{}
	sRunner := pipeline.NewSplitterRunner("HekaFramingSplitter", splitter, srConfig)
	return sRunner, nil
}

func main() {
	flagMatch := flag.String("match", "TRUE", "message_matcher filter expression")
	flagOutput := flag.String("output", "", "output filename, defaults to stdout")
	flagTail := flag.Bool("tail", false, "don't exit on EOF")
	flagOffset := flag.Int64("offset", 0, "starting offset for the input file in bytes")
	flagMaxMessageSize := flag.Uint64("max-message-size", 4*1024*1024, "maximum message size in bytes")
	flag.Parse()

	errorCodes := make(map[string]int)

	if *flagMaxMessageSize < math.MaxUint32 {
		maxSize := uint32(*flagMaxMessageSize)
		message.SetMaxMessageSize(maxSize)
	} else {
		fmt.Fprintf(os.Stderr, "Message size is too large: %d\n", flagMaxMessageSize)
		os.Exit(8)
	}

	var err error
	var match *message.MatcherSpecification
	if match, err = message.CreateMatcherSpecification(*flagMatch); err != nil {
		fmt.Fprintf(os.Stderr, "Match specification - %s\n", err)
		os.Exit(2)
	}

	var out *os.File
	if "" == *flagOutput {
		out = os.Stdout
	} else {
		if out, err = os.OpenFile(*flagOutput, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644); err != nil {
			fmt.Fprintf(os.Stderr, "%s\n", err)
			os.Exit(4)
		}
		defer out.Close()
	}

	var file *os.File
	for _, filename := range flag.Args() {
		if file, err = os.Open(filename); err != nil {
			fmt.Fprintf(os.Stderr, "%s\n", err)
			os.Exit(3)
		}
		defer file.Close()

		var offset int64
		if offset, err = file.Seek(*flagOffset, 0); err != nil {
			fmt.Fprintf(os.Stderr, "%s\n", err)
			os.Exit(5)
		}

		sRunner, err := makeSplitterRunner()
		if err != nil {
			fmt.Println(err)
			os.Exit(7)
		}
		msg := new(message.Message)
		var processed, matched int64

		fmt.Fprintf(os.Stderr, "Input:%s  Offset:%d  Match:%s  Format:%s  Tail:%t  Output:%s\n",
		flag.Arg(0), *flagOffset, *flagMatch, *flagTail, *flagOutput)
		for true {
			n, record, err := sRunner.GetRecordFromStream(file)
			if n > 0 && n != len(record) {
				fmt.Fprintf(os.Stderr, "Corruption detected at offset: %d bytes: %d\n", offset, n-len(record))
			}
			if err != nil {
				if err == io.EOF {
					if !*flagTail {
						break
					}
					time.Sleep(time.Duration(500) * time.Millisecond)
				} else {
					break
				}
			} else {
				if len(record) > 0 {
					processed += 1
					headerLen := int(record[1]) + message.HEADER_FRAMING_SIZE
					if err = proto.Unmarshal(record[headerLen:], msg); err != nil {
						fmt.Fprintf(os.Stderr, "Error unmarshalling message at offset: %d error: %s\n", offset, err)
						continue
					}

					if !match.Match(msg) {
						continue
					}
					matched += 1

					for _, item := range msg.Fields {
						value := fmt.Sprintf("%+v", item.GetValue())
						if *item.Name == "errorCode" {
							errorCodes[value] = errorCodes[value] + 1
						}
					}
				}
			}
			offset += int64(n)
		}
		fmt.Fprintf(os.Stderr, "Processed: %d, matched: %d messages\n", processed, matched)
	}
	for code, count := range errorCodes {
		fmt.Fprintf(out, "%s, %d\n", code, count)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(6)
	}
}

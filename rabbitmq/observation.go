package rabbitmq

import "github.com/faustbrian/go-rabbitmq-streams"

func safeObserve(observer rabbitstream.Observer, observation rabbitstream.Observation) {
	if observer == nil {
		return
	}
	defer func() { _ = recover() }()
	observer.Observe(observation)
}

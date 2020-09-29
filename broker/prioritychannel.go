package broker

type PriorityChannel struct {
	Out      chan interface{}
	Urgent   chan interface{}
	Critical chan interface{}
	Normal   chan interface{}
	stopCh   chan struct{}
}

func NewPriorityChannel() *PriorityChannel {
	pc := PriorityChannel{}
	pc.Out = make(chan interface{})
	pc.Urgent = make(chan interface{}, 1)
	pc.Critical = make(chan interface{}, 1)
	pc.Normal = make(chan interface{}, 1)
	pc.stopCh = make(chan struct{})

	pc.start()
	return &pc
}

func (pc *PriorityChannel) Close() {
	close(pc.stopCh)
	close(pc.Normal)
	close(pc.Critical)
	close(pc.Urgent)
	close(pc.Out)
}

func (pc *PriorityChannel) start() {
	go func() {
		for {
			select {
			case s := <-pc.Urgent:
				pc.Out <- s
				continue
			case <-pc.stopCh:
				return
			default:
			}

			select {
			case s := <-pc.Urgent:
				pc.Out <- s
				continue
			case s := <-pc.Critical:
				pc.Out <- s
				continue
			case <-pc.stopCh:
				return
			default:
			}

			select {
			case s := <-pc.Urgent:
				pc.Out <- s
			case s := <-pc.Critical:
				pc.Out <- s
			case s := <-pc.Normal:
				pc.Out <- s
			case <-pc.stopCh:
				return
			}
		}
	}()
}

package tcp

type MasterServer struct {
	*BaseServer
}

func (m *MasterServer) Start() {
	m.StartListener()
}

func (m *MasterServer) Stop() {
	m.StopListener()
}

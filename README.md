# IpfsGateWay
Get the CID data through the Saturn CDN network


go build main.go

./main QmYuvnFKLi8cf7KjZn9NKQvYKamSJ9V9MZRyfakkRL5pPv ./test.txt

获取saturn nodes
// func getOrchestratorEndpoint() []string {
// 	DefaultOrchestratorEndpoint := "https://orchestrator.strn.pl/nodes/nearby?count=1000"
// 	OrchestratorClient := http.DefaultClient
// 	resp, err := OrchestratorClient.Get(DefaultOrchestratorEndpoint)
// 	if err != nil {
// 		fmt.Println(err)
// 	}
// 	defer resp.Body.Close()
// 	responses := make([]string, 0)
// 	if err := json.NewDecoder(resp.Body).Decode(&responses); err != nil {
// 		fmt.Println(err)
// 	}
// 	fmt.Print("got backends from orchestrator", "cnt", len(responses), "endpoint", DefaultOrchestratorEndpoint)
// 	return responses
// }
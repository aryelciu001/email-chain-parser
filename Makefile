MINIKUBE_IP := $(shell minikube ip 2>/dev/null)

cluster-up:
	minikube start --driver=docker
	minikube addons enable ingress

copy-data:
	tar -C test -czf /tmp/test-data.tar.gz .
	minikube cp /tmp/test-data.tar.gz /tmp/test-data.tar.gz
	minikube ssh "sudo mkdir -p /data/test && sudo tar -C /data/test -xzf /tmp/test-data.tar.gz"

build:
	minikube image build -t server:local ./k8s-local/server
	minikube image build -t documents-consumer:local ./k8s-local/workers/documents-consumer
	minikube image build -t emails-consumer:local ./k8s-local/workers/emails-consumer

deploy: copy-data
	kubectl wait --namespace ingress-nginx --for=condition=ready pod --selector=app.kubernetes.io/component=controller --timeout=120s
	kubectl apply -f k8s-local/manifests/namespace.yaml
	kubectl delete jobs -n demo --all --ignore-not-found
	kubectl apply -R -f k8s-local/manifests/

tunnel:
	minikube tunnel

ingest:
	curl -s -X POST http://my.local/api/ingest \
		-H "Content-Type: application/json" \
		-d "{\"doc_url\":\"$(DOC)\"}"

ingest-all:
	ls test/ | xargs -P8 -I{} curl -s -X POST http://my.local/api/ingest \
		-H "Content-Type: application/json" \
		-d "{\"doc_url\":\"{}\"}"

test:
	curl -s http://my.local/api/health || echo "Run 'minikube tunnel' in another terminal first, then add '127.0.0.1 my.local' to /etc/hosts"

cluster-down:
	kubectl delete namespace demo
	minikube stop

# kubectl convenience

get-apps:
	kubectl -n demo get deployments

log-server:
	kubectl -n demo logs -l app=server

log-document:
	kubectl -n demo logs -l app=documents-consumer

log-email:
	kubectl -n demo logs -l app=emails-consumer

restart-document:
	kubectl -n demo rollout restart deployment/documents-consumer

ready: log-document log-email

restart-server: build
	kubectl -n demo rollout restart deployment/server
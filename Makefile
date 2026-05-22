MINIKUBE_IP := $(shell minikube ip 2>/dev/null)

cluster-up:
	minikube start --driver=docker
	minikube addons enable ingress

build:
	minikube image build -t server:local ./k8s-local/server
	minikube image build -t documents-consumer:local ./k8s-local/workers/documents-consumer
	minikube image build -t emails-consumer:local ./k8s-local/workers/emails-consumer

deploy:
	kubectl apply -f k8s-local/manifests/namespace.yaml
	kubectl delete jobs -n demo --all --ignore-not-found
	kubectl apply -R -f k8s-local/manifests/

tunnel:
	minikube tunnel

status:
	kubectl -n demo get pods,svc,ingress

test:
	curl -s http://demo.local/ || echo "Run 'minikube tunnel' in another terminal first, then add '127.0.0.1 demo.local' to /etc/hosts"

scale:
	kubectl -n demo scale deployment/python-server --replicas=4

cluster-down:
	kubectl delete namespace demo
	minikube stop

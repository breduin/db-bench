PROJECT_NAME = test

build:
	docker compose build

stop:
	docker compose down

restart:
	docker compose restart

rerun:
	docker compose down
	docker compose up -d --build

run:
	docker compose up -d --build

log-redis:
	docker logs $(PROJECT_NAME)-redis -f --tail=100

log-scylla:
	docker logs $(PROJECT_NAME)-scylla -f --tail=100

log-garnet:
	docker logs $(PROJECT_NAME)-garnet -f --tail=100

log-clickhouse:
	docker logs $(PROJECT_NAME)-clickhouse -f --tail=100

log-keydb:
	docker logs $(PROJECT_NAME)-keydb -f --tail=100

log-postgres:
	docker logs $(PROJECT_NAME)-postgres -f --tail=100

log-mongo:
	docker logs $(PROJECT_NAME)-mongo -f --tail=100

log-tester:
	docker logs $(PROJECT_NAME)-tester -f --tail=100

shell-tester:
	docker exec -it $(PROJECT_NAME)-tester /bin/bash

shell-redis:
	docker exec -it $(PROJECT_NAME)-redis /bin/bash

shell-scylla:
	docker exec -it $(PROJECT_NAME)-scylla /bin/bash

shell-garnet:
	docker exec -it $(PROJECT_NAME)-garnet /bin/bash

shell-clickhouse:
	docker exec -it $(PROJECT_NAME)-clickhouse /bin/bash

shell-keydb:
	docker exec -it $(PROJECT_NAME)-keydb /bin/bash

shell-postgres:
	docker exec -it $(PROJECT_NAME)-postgres /bin/bash

shell-mongo:
	docker exec -it $(PROJECT_NAME)-mongo /bin/bash

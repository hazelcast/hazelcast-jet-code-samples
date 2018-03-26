# docker-compose-hazelcast-jet makefile

# Environment Variables
CONTAINER_SERVER = hazelcast-jet
CONTAINER_CLIENT = hazelcast-jet-submit

export JOB_JAR= /jars/$(shell ls -tr jars| tail -2)

.PHONY: up

prep :
	echo  $(JOB_JAR) file will be submitted to the cluster....

pull :
	docker-compose -f hazelcast.yml pull

up : prep pull
	docker-compose -f hazelcast.yml up -d

scale :
	docker-compose -f hazelcast.yml up --scale $(CONTAINER_SERVER)=5 -d

down :
	docker-compose -f hazelcast.yml down

restart :
	docker-compose -f hazelcast.yml restart $(CONTAINER_CLIENT)  

logServer :
	docker-compose -f hazelcast.yml logs $(CONTAINER_SERVER)

logClient :
	docker-compose -f hazelcast.yml logs $(CONTAINER_CLIENT)

tailServer :
	docker-compose -f hazelcast.yml logs -f $(CONTAINER_SERVER)

tailClient :
	docker-compose -f hazelcast.yml logs -f $(CONTAINER_CLIENT)


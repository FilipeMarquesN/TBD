# Advanced Databases
# Book Recommendations Dataset

Bundled with a docker compose file in case you want to run docker to test both databases at once.
If you're running some other software such as the actual MySQL and MongoDB, then adjust the values in the .env file so python can properly find these points

##### Docker Compose
Docker compose file included for simplicity to boot up everything at once:
###### Running the containers:
```bash
docker-compose up -d
```
###### Shutting down the containers:
```bash
docker-compose down
```

WARNING REGARDING DOCKER: delete docker_volumes folder whenever you change the definitions in the env folder

##### Included Configuration File
The included .env file contains values which map the necessary parameters to build a proper connection string within the python script. Be sure to check this file before attempting to execute this application!
WARNING: do not touch the variables inside the "paths" unless you know what you're doing!

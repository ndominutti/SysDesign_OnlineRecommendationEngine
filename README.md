# Parte Airflow
* Cuando creamos la EC2 tenemos que instalar docker en el ubuntu
* Clonamos el repo
* buildeamos la imagen: sudo docker build -t pa_udesa .
* corremos el container: sudo docker run -p 8000:8000 --name pa_udesa -it pa_udesa /bin/bash
* dentro del container: airflow webserver --port 8000 &
                        airflow scheduler &
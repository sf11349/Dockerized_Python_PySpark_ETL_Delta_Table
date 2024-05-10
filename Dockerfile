# get parent image
#FROM python
FROM jupyter/pyspark-notebook

# set the working dirctory and copy all
WORKDIR /sf_de_assignment
COPY . .

# install the dependencies
RUN pip install -r requirements.txt

# optional for docker desktop
EXPOSE 4000

# run the python file
CMD ["python3","etl.py"]
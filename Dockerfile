FROM public.ecr.aws/lambda/python:3.8


COPY requirements.txt ./
RUN pip install -r requirements.txt

COPY aws ./aws
COPY app.py ./

# You can overwrite command in `serverless.yml` template
CMD ["app.handler"]

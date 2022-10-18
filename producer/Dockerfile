FROM python:3.8
ENV PATH /usr/src/app/bin:$PATH
WORKDIR /usr/src/app

COPY requirements.txt ./
#RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir git+https://github.com/IL2C/instaloader.git@login-fix

COPY ./ ./
CMD [ "python3","-u", "instap-producer.py" ]
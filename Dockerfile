# Use an official Python runtime as a parent image
FROM python:3.9

# Set the working directory to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install -r requirements.txt  # If you have a requirements file

# Define environment variables if necessary
ENV SNOWFLAKE_USERNAME=comicsvibe
ENV SNOWFLAKE_PASSWORD=Comicsvibe123!
# Add other environment variables as needed

# Run your script when the container launches
CMD ["python", "scraper.py","table_creator.py","data_uploader.py"]

import os

from src import app

# Run this command to run this application in Debug mode
# docker run -p 8080:8080 -e DEBUG=1 <image-name>
if __name__ == "__main__":
    app.run(
        host="0.0.0.0", port=8080, debug=os.environ.get("DEBUG") == "1"
    )  # 0.0.0.0 (convention to dockerize) enables connection for all web based interfaces (easier to expose container to local host)

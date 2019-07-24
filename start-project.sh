#!/usr/bin/env python3
gnome-terminal -e "python3 flask.py"
gnome-terminal -e "python3 SparkStreaming.py" 
gnome-terminal -e "python3 kafkaProducer.py" 
python3 -m webbrowser "http://localhost:5001/"

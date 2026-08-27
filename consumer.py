from confluent_kafka import Consumer
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from contextlib import asynccontextmanager
import json, asyncio, threading

active_connections: set[WebSocket] = set() # Set to store active connections in

def main(loop: asyncio.AbstractEventLoop, stop_event: threading.Event):
    consumer_config = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'coinbase_cluster',
        'auto.offset.reset': 'latest' # Start reading from the latest offset
    }

    with Consumer(consumer_config) as consumer:
        consumer.subscribe(["crypto-prices"]) # Connect to the topic

        while not stop_event.is_set():
            raw_msg = consumer.poll(1)
            if(raw_msg) is None:
                print('Waiting...')
                continue
            elif raw_msg.error() is not None:
                print('ERROR:', raw_msg.error())
                continue
            else:
                msg = json.loads(raw_msg.value().decode('utf-8')) # Interpret the JSON, first decode from utf then read
                key = msg.get('symbol')
                value = msg.get('price')
                time = msg.get('time')

                tick = {"symbol": key, "price": value, "time": time} # Put everything into a format (dict) that can be repackaged into JSON

                asyncio.run_coroutine_threadsafe(broadcast(tick), loop) # COME BACK TO THIS

async def broadcast(message: dict):

    if not active_connections: # If there aren't currently any active connections, dont bother
        return

    data = json.dumps(message) # Convert tick to JSON
    dead = [] # Keep track of which connections to discard from the set

    for ws in active_connections:
        try: # Send data to client
            await ws.send_text(data)
        except Exception: # If the client can't be reach mark it for removal
            dead.append(ws)
    for ws in dead:
        active_connections.discard(ws)

@asynccontextmanager
async def lifespan(app: FastAPI):
    loop = asyncio.get_running_loop()
    app.state.stop_event = threading.Event()
    app.state.consumer_thread = threading.Thread(
        target = main,
        args = (loop, app.state.stop_event),
        daemon = True
    )
    app.state.consumer_thread.start() # Run main concurrently with the FastAPI

    yield # Everything before this is run at startup, everything after during termination

    app.state.stop_event.set() # Set flag in main to True, stops polling the broker and begins shutdown of main
    app.state.consumer_thread.join (timeout = 5) # Give main 5 seconds to close the consumer

app = FastAPI(lifespan=lifespan)

@app.get("/")
async def root():
    return {"message": "API server is running"}

@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket):
    await websocket.accept() # Websocket handshake
    active_connections.add(websocket) # Add current client to active connections list

    try:
        while True:
            await websocket.receive_text() # This doesnt actually do anything, instead it needs to be running so that asyncio will be able to know when the client disconnects, and so the connection must close
    except WebSocketDisconnect:
        pass
    finally:
        active_connections.discard(websocket)

# if __name__ == "__main__":
#     try:
#         main()
#     except KeyboardInterrupt:
#         print('\nTERMINATED CONSUMER')
    
# Stomp client via WebSocket for Kotlin/Android with auto reconnect

## Example 

``` kotlin

lateinit var stompConnection: Disposable
lateinit var topic: Disposable

val url = "ws://example.com/endpoint"
val reconnectIntervalMillis = 5000L //auto reconnect after 5 secounds
val client = OkHttpClient()
val customConnectionHeaders = HashMap<String, String>() //you can pass some headers like token to connection

val stomp = StompClient(url, client, reconnectIntervalMillis)

// connect
// pass null if you don't need any header
stompConnection = stomp.connect(customConnectionHeaders).subscribe {
    when (it.type) {
        Event.Type.OPENED -> {
            //if you want to auto subscribe to topic, just call subscribe here like:
            //topic = stomp.subscribe("/destination").subscribe { Log.i(TAG, it) }
        }
        Event.Type.CLOSED -> {
            //No need to try reconnect
        }
        Event.Type.ERROR -> {

        }
    }
}

// subscribe
topic = stomp.subscribe("/destination").subscribe { message -> Log.i(TAG, message) }

// unsubscribe
topic.dispose()

// send
stomp.send("/destination", "dummy message").subscribe {
    if (it) {
    }
}

// disconnect
stompConnection.dispose()
     
```

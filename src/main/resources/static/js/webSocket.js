$(function(){
	socket.init();
});
let basePath = "ws://localhost:8080/seckill/";
socket = {
	webSocket : "",
	init : function() {
		//userId：自行追加
		if ('WebSocket' in window) {
			this.webSocket = new WebSocket(basePath+'websocket/1');
		}
		else if ('MozWebSocket' in window) {
			this.webSocket = new MozWebSocket(basePath+"websocket/1");
		}
		else {
			this.webSocket = new SockJS(basePath+"sockjs/websocket");
		}
		// 当 webSocket 发生错误时，调用
		this.webSocket.onerror = function(event) {
			console.error("======> websocket 连接发生错误，请刷新页面重试！");
		};

		this.webSocket.onopen = function(event) {
			console.log("======> websocket 连接成功！")
		};
		this.webSocket.onmessage = function(event) {
			console.log("======> websocket服务器返回数据");
			//判断秒杀是否成功、自行写逻辑
			let message = event.data;
			console.log("======> message", message)
		};
	}
}
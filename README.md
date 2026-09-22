// client.js — браузерная сторона моста (референс для осциллографа).
// Данные Modbus идут ТОЛЬКО бинарными фреймами; JSON — служебные статусы.
 
const ws = new WebSocket("ws://localhost:8080/proxy");
ws.binaryType = "arraybuffer"; // получаем ArrayBuffer, а не Blob
▸ Данные
Никаких строк — только байты

binaryType="arraybuffer" + Uint8Array: байты Modbus не проходят через UTF-8, где недопустимые последовательности ломали бы CRC. Отправка — ws.send(frame.buffer).

 
ws.onopen = () => {
  // Резервный handshake: сообщаем прокси адрес ПЛК.
  // Можно и иначе — сразу открыть:
  // ws://localhost:8080/proxy?host=192.168.1.10&port=501
  ws.send(JSON.stringify({ host: "192.168.1.10", port: 501 }));
▸ Handshake
Адрес ПЛК — текстовым фреймом

Первым сообщением говорим прокси, куда подключаться. Проще — сразу открыть ws://localhost:8080/proxy?host=192.168.1.10&port=501, тогда handshake не нужен.

};
 
ws.onmessage = (ev) => {
  if (ev.data instanceof ArrayBuffer) {
▸ Служебные
Разбираем только текстовые фреймы

Бинарные фреймы — это данные ПЛК, отдаём их осциллографу как есть. Текстовые — статусы прокси; proxy_error означает, что TCP-соединение с контроллером разорвано.

    // Бинарный фрейм = сырые байты от ПЛК (ответ Modbus RTU).
    const bytes = new Uint8Array(ev.data);
    console.log("от ПЛК:", bytes);
    // ...отдаём в парсер осциллографа
  } else {
    // Текстовый фрейм = служебное сообщение прокси.
    const msg = JSON.parse(ev.data);
    if (msg.type === "proxy_error") {
      console.error("прокси сообщает:", msg.message);
    }
  }
};
 
// Пример запроса: чтение регистров (FC 03), адрес 1, 4 слова с 0x0000.
// CRC16 (Modbus) считает ваша библиотека — прокси байты не трогает.
const req = new Uint8Array([0x01, 0x03, 0x00, 0x00, 0x00, 0x04, 0x45, 0xC9]);
ws.send(req.buffer); // <- бинарный фрейм
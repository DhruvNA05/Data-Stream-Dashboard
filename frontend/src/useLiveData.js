import {useEffect, useRef, useState} from 'react'

const MAX_POINTS = 100;

export function useLiveData(url) {
    const [dataBySymbol, setDataSymbol] = useState({});
    const wsRef = useRef(null);

    useEffect(() => {

        const ws = new WebSocket(url);
        wsRef.current = ws;

        ws.onopen = () => {
            console.log("Websocket connected");
        };

        ws.onmessage = (event) => {

            const point = JSON.parse(event.data);
            const {symbol, price, time} = point;

            setDataBySymbol((prev) => {
                const existing = prev[symbol] || [];
                const updated = [...existing, {time, price}].slice(-MAX_POINTS);
                return {...prev, [symbol]: updated};
            });
        };
        ws.onerror = (err) => console.error('Websocket error:', err);
        return () => {
            ws.close();
        }
    }, [url]);
    return dataBySymbol;
}
import {useEffect, useRef, useState} from 'react'

const MAX_POINTS = 100;

export function useLiveData(url) {
    const [dataBySymbol, setDataSymbol] = useState({});
    const wsRef = useRef(null);

    useEffect(() => {

        const ws = new WebSocket(url);
        wsRef.current = ws;

        ws.onmessage = (event) => {

            const point = JSON.parse(event.data);
            const {symbol, price, timestamp} = point;

            setDataBySymbol((prev) => {
                const existing = prev[symbol] || [];
                const updated = [...existing, {time: timestamp, price}].slice(-MAX_POINTS);
                return {...prev, [symbol]: updated};
            });
        };
        ws.oneerror = (err) => console.error('Websocket error:', err);
        return () => {
            ws.close();
        }
    }, [url]);
    return dataBySymbol;
}
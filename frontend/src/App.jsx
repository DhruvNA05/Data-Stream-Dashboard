import { useState } from 'react'
import heroImg from './assets/hero.png'
import reactLogo from './assets/react.svg'
import viteLogo from './assets/vite.svg'
import './App.css'
import { useLiveData } from './useLiveData'
import { LineChart, Line, XAxis, YAxis, Tooltip, CartesianGrid, ResponsiveContainer } from 'recharts'

const WS_URL = 'ws://localhost:8000/ws';

export default function App() {
  const dataBySymbol = useLiveData(WS_URL);
  const symbols = Object.keys(dataBySymbol);
  return(
    <div style = {{padding: 24}}>
      <h1>Live Prices</h1>
      {symbols.length === 0 && <p>Waiting for data...</p>}
      {symbols.map((symbol) => (
        <div key = {symbol} style = {{ marginBottom: 32}}>
          <h3>{symbol} </h3>
          <ResponsiveContainer width = "100%" height = {250}>
            <LineChart data = {dataBySymbol[symbol]}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis
                dataKey="time"
                tickFormatter={(t) => new Date(t).toLocaleTimeString()}
                />
              <YAxis domain = {['auto', 'auto']} />
              <Tooltip labelFormatter={(t) => new Date(t).toLocaleTimeString()} />
              <Line type= "monotone" dataKey= "price" dot = {false} isAnimationActive = {false} />
            </LineChart>
          </ResponsiveContainer>
        </div>
      ))}
  </div>
  );
}
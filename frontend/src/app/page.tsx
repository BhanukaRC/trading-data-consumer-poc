"use client";

import { useEffect, useState } from "react";

interface PnL {
  startTime: string;
  endTime: string;
  pnl: number;
}

export default function HomePage() {
  const [openPosition, setOpenPosition] = useState<string>("0.0");
  const [pnls, setPnls] = useState<PnL[]>([]);
  const [health, setHealth] = useState<string>("UNKNOWN");

  // Fetch open position via SSE
  useEffect(() => {
    const eventSource = new EventSource("http://localhost:3001/open-position");
    eventSource.onmessage = (event) => {
      setOpenPosition(event.data);
    };
    return () => eventSource.close();
  }, []);

  // Fetch PnL via SSE
  useEffect(() => {
    const eventSource = new EventSource("http://localhost:3001/pnl");
    eventSource.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        setPnls(data);
      } catch (e) {
        console.error("Failed to parse PnL data:", e);
      }
    };
    return () => eventSource.close();
  }, []);

  // Fetch health status
  useEffect(() => {
    fetch("http://localhost:3001/health")
      .then((res) => res.json())
      .then((data) => setHealth(data.status))
      .catch(() => setHealth("ERROR"));
  }, []);

  const formatCurrency = (value: number) => {
    return new Intl.NumberFormat("en-GB", {
      style: "currency",
      currency: "EUR",
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    }).format(value);
  };

  return (
    <main className="min-h-screen bg-gray-900 text-white p-8">
      <div className="max-w-6xl mx-auto">
        <h1 className="text-4xl font-bold mb-8">Trading Data Consumer PoC</h1>
        
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
          {/* Open Position */}
          <div className="bg-gray-800 rounded-lg p-6">
            <h2 className="text-xl font-semibold mb-4">Open Position</h2>
            <p className="text-3xl font-bold">{openPosition} MW</p>
          </div>

          {/* Service Health */}
          <div className="bg-gray-800 rounded-lg p-6">
            <h2 className="text-xl font-semibold mb-4">Status</h2>
            <p className="text-2xl">{health}</p>
          </div>

          {/* PnL Summary */}
          <div className="bg-gray-800 rounded-lg p-6">
            <h2 className="text-xl font-semibold mb-4">Profit and Loss</h2>
            {pnls.length === 0 ? (
              <p className="text-gray-400">Loading...</p>
            ) : (
              <div className="space-y-3">
                {pnls.map((pnl, index) => {
                  const labels = ["Last Market Period", "Last 1 Minute", "Last 5 Minutes"];
                  const isPositive = pnl.pnl >= 0;
                  return (
                    <div key={index} className="border border-gray-700 rounded p-3">
                      <div className="text-sm text-gray-400 mb-1">{labels[index]}</div>
                      <div className={`text-xl font-bold ${isPositive ? "text-green-400" : "text-red-400"}`}>
                        {formatCurrency(pnl.pnl)}
                      </div>
                      <div className="text-xs text-gray-500 mt-1">
                        {pnl.startTime} - {pnl.endTime}
                      </div>
                    </div>
                  );
                })}
              </div>
            )}
          </div>
        </div>
      </div>
    </main>
  );
}

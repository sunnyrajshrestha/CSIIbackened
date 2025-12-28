const express = require('express');
const cors = require('cors');
const app = express();

app.use(cors());
app.use(express.json());

// Store latest sensor data for each room
let sensorData = {};

// Endpoint for ESP8266 to POST data
app.post('/api/sensor-data', (req, res) => {
  const { roomId, buildingId, floor, temperature, humidity, wifiDevices, occupancy, timestamp, sensorStatus } = req.body;
  
  console.log(`📡 Data from Room ${roomId}:`, req.body);
  
  // Store data by roomId
  sensorData[roomId] = {
    roomId,
    buildingId,
    floor,
    temperature,
    humidity,
    wifiDevices,
    occupancy,
    timestamp,
    sensorStatus,
    lastUpdate: new Date().toISOString()
  };
  
  res.json({ success: true, message: 'Data received' });
});

// Endpoint for Dashboard to GET data for specific room
app.get('/api/rooms/:roomId', (req, res) => {
  const roomId = req.params.roomId;
  const data = sensorData[roomId];
  
  if (data) {
    res.json(data);
  } else {
    res.status(404).json({ error: 'Room not found' });
  }
});

// Endpoint for Dashboard to GET all rooms data
app.get('/api/rooms', (req, res) => {
  res.json(sensorData);
});

// Health check
app.get('/api/health', (req, res) => {
  res.json({ 
    status: 'online', 
    rooms: Object.keys(sensorData).length,
    timestamp: new Date().toISOString()
  });
});

const PORT = process.env.PORT || 3001;
app.listen(PORT, () => {
  console.log(`✅ API Server running on port ${PORT}`);
  console.log(`📡 ESP8266 POST to: http://localhost:${PORT}/api/sensor-data`);
  console.log(`🖥️  Dashboard GET from: http://localhost:${PORT}/api/rooms`);
});
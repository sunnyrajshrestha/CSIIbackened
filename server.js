const express = require('express');
const cors = require('cors');
const { MongoClient } = require('mongodb');
const app = express();

app.use(cors());
app.use(express.json());

// In-memory storage (for fast real-time access)
let sensorData = {};

// MongoDB Configuration
const MONGO_URI = process.env.MONGO_URI || 'mongodb+srv://sunnys_db_user:Y3EZnDknjIJBIeuO@digital-twin.gq1elir.mongodb.net/?retryWrites=true&w=majority&appName=digital-twin';
const DB_NAME = 'csii-iot';
const COLLECTION_NAME = 'sensor-readings';

let db;
let collection;

// Connect to MongoDB and create time-series collection
async function connectDB() {
  try {
    const client = new MongoClient(MONGO_URI);
    await client.connect();
    db = client.db(DB_NAME);
    
    // Check if time-series collection exists
    const collections = await db.listCollections({ name: COLLECTION_NAME }).toArray();
    
    if (collections.length === 0) {
      // Create TIME-SERIES collection
      await db.createCollection(COLLECTION_NAME, {
        timeseries: {
          timeField: 'timestamp',
          metaField: 'metadata',
          granularity: 'seconds'
        }
      });
      console.log('✅ Created time-series collection');
    }
    
    collection = db.collection(COLLECTION_NAME);
    console.log('✅ Connected to MongoDB Atlas (Time-Series)');
  } catch (error) {
    console.error('❌ MongoDB connection error:', error);
  }
}

connectDB();

// POST endpoint - receive sensor data
app.post('/api/sensor-data', async (req, res) => {
  const { roomId, buildingId, floor, temperature, humidity, wifiDevices, occupancy, timestamp, sensorStatus } = req.body;
  
  console.log(`📡 Data from Room ${roomId}:`, req.body);
  
  // Store in RAM for real-time dashboard
  sensorData[roomId] = { 
    roomId, buildingId, floor, temperature, humidity, 
    wifiDevices, occupancy, timestamp, sensorStatus, 
    lastUpdate: new Date().toISOString() 
  };
  
  // Store in MongoDB TIME-SERIES collection
  try {
    await collection.insertOne({
      timestamp: new Date(),
      metadata: {
        roomId,
        buildingId,
        floor: floor.toString()
      },
      temperature,
      humidity,
      wifiDevices,
      occupancy,
      sensorStatus
    });
    console.log('✅ Saved to MongoDB');
  } catch (error) {
    console.error('❌ MongoDB Error:', error);
  }
  
  res.json({ success: true, message: 'Data received and stored' });
});

// GET current data (from RAM - fast!)
app.get('/api/rooms/:roomId', (req, res) => {
  const data = sensorData[req.params.roomId];
  if (data) res.json(data); 
  else res.status(404).json({ error: 'Room not found' });
});

app.get('/api/rooms', (req, res) => { 
  res.json(sensorData); 
});

// GET historical data (from MongoDB)
app.get('/api/history/:roomId', async (req, res) => {
  const { roomId } = req.params;
  const hours = parseInt(req.query.hours) || 24;
  
  try {
    const startTime = new Date(Date.now() - hours * 60 * 60 * 1000);
    
    const data = await collection.find({
      'metadata.roomId': roomId,
      timestamp: { $gte: startTime }
    })
    .sort({ timestamp: 1 })
    .limit(1000)
    .toArray();
    
    res.json(data);
  } catch (error) {
    console.error('Query error:', error);
    res.status(500).json({ error: 'Failed to fetch history' });
  }
});

// GET statistics
app.get('/api/stats/:roomId', async (req, res) => {
  const { roomId } = req.params;
  const hours = parseInt(req.query.hours) || 24;
  
  try {
    const startTime = new Date(Date.now() - hours * 60 * 60 * 1000);
    
    const stats = await collection.aggregate([
      { $match: { 'metadata.roomId': roomId, timestamp: { $gte: startTime } } },
      { $group: {
        _id: null,
        avgTemp: { $avg: '$temperature' },
        minTemp: { $min: '$temperature' },
        maxTemp: { $max: '$temperature' },
        avgHumidity: { $avg: '$humidity' },
        avgOccupancy: { $avg: '$occupancy' },
        maxOccupancy: { $max: '$occupancy' },
        totalReadings: { $sum: 1 }
      }}
    ]).toArray();
    
    res.json(stats[0] || {});
  } catch (error) {
    console.error('Stats error:', error);
    res.status(500).json({ error: 'Failed to fetch stats' });
  }
});

app.get('/api/health', (req, res) => {
  res.json({ 
    status: 'online', 
    rooms: Object.keys(sensorData).length, 
    timestamp: new Date().toISOString(),
    database: db ? 'MongoDB Time-Series connected' : 'Disconnected'
  });
});

const PORT = process.env.PORT || 3001;
app.listen(PORT, () => console.log(`✅ Server running on port ${PORT}`));

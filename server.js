const WebSocket = require('ws');
const url = require('url');
const PORT = process.env.PORT || 3000;
const wss = new WebSocket.Server({
    port: PORT,
    handleProtocols: (protocols, req) => {
        if (protocols) {
            if (protocols instanceof Set && protocols.size > 0) {
                return Array.from(protocols)[0];
            } else if (Array.isArray(protocols) && protocols.length > 0) {
                return protocols[0];
            }
        }
        return false;
    }
});

const rooms = new Map();
const peerToRoom = new Map();

console.log(`=== Room Server running on port ${PORT} ===`);

wss.on('connection', (ws, req) => {
    const clientIP = req.headers['x-forwarded-for'] || req.connection.remoteAddress;
    ws._ip = clientIP;

    console.log(`[+] Connection from: ${clientIP}`);
    ws._ip = clientIP;

    ws.on('message', (data, isBinary) => {
        try {
            const msg = JSON.parse(data.toString());
            handleMessage(ws, msg, clientIP);
        } catch (e) {
            console.error('Parse error:', e.message);
        }
    });

    ws.on('close', () => {
        console.log(`[-] Disconnected`);
        leaveRoom(ws);
    });
});

function handleMessage(ws, msg, clientIP) {
    const type = msg.type;

    if (type === 'create_room') {
        const roomId = generateId();
        rooms.set(roomId, {
            name: msg.name || 'Room',
            map: msg.map || 'de_dust2',
            host_ws: ws,
            host_ip: clientIP,
            players: [{ ws: ws, name: msg.player || 'Host', id: 1, ready: true, ping: 0 }],
            max_players: msg.max_players || 4
        });
        peerToRoom.set(ws, roomId);
        ws.roomId = roomId;
        ws.playerId = 1;
        ws.playerName = msg.player || 'Host';
        send(ws, { type: 'room_created', room_id: roomId, your_id: 1, map: msg.map || 'de_dust2' });
        broadcastRooms();
        console.log('[ROOM] Created: ' + roomId + ' "' + msg.name + '" by ' + clientIP);

    } else if (type === 'list_rooms') {
        send(ws, { type: 'room_list', rooms: getRoomList() });

    } else if (type === 'join_room') {
        const room = rooms.get(msg.room_id);
        if (!room) { send(ws, { type: 'error', message: 'Room not found' }); return; }
        if (room.players.length >= room.max_players) { send(ws, { type: 'error', message: 'Room is full' }); return; }
        const playerId = room.players.length + 1;
        room.players.push({ ws: ws, name: msg.player || 'Player', id: playerId, ready: false, ping: 0 });
        peerToRoom.set(ws, msg.room_id);
        ws.roomId = msg.room_id;
        ws.playerId = playerId;
        ws.playerName = msg.player || 'Player';
        send(ws, { type: 'room_joined', room_id: msg.room_id, your_id: playerId, room_name: room.name, map: room.map || 'de_dust2', host_ip: room.host_ip });
        send(room.host_ws, { type: 'peer_joined', peer_id: playerId, name: msg.player || 'Player' });
        broadcastPlayerList(room);
        broadcastRooms();
        console.log('[ROOM] ' + msg.player + ' joined ' + msg.room_id);

    } else if (type === 'ready') {
        const room = rooms.get(ws.roomId);
        if (!room) return;
        const player = room.players.find(function(p) { return p.ws === ws; });
        if (player) {
            player.ready = msg.ready || false;
            console.log('[READY] Player ' + player.name + ' ready=' + player.ready);
            broadcastPlayerList(room);
        }

    } else if (type === 'ping') {
        send(ws, { type: 'pong', time: msg.time });

    } else if (type === 'report_ping') {
        const room = rooms.get(ws.roomId);
        if (!room) return;
        const pinger = room.players.find(function(p) { return p.ws === ws; });
        if (pinger) {
            pinger.ping = msg.ping || 0;
            broadcastPlayerList(room);
        }

    } else if (type === 'kick_player') {
        const room = rooms.get(ws.roomId);
        console.log('[KICK] roomId=' + ws.roomId + ' room=' + !!room);
        if (!room) { console.log('[KICK] No room'); return; }
        if (room.host_ws !== ws) { console.log('[KICK] Not host'); return; }
        var target_id = msg.player_id || msg.peer_id;
        console.log('[KICK] target_id=' + target_id + ' ids=' + JSON.stringify(room.players.map(function(p){return p.id})));
        var target = null;
        for (var i = 0; i < room.players.length; i++) {
            if (room.players[i].id === target_id) { target = room.players[i]; break; }
        }
        if (!target) { console.log('[KICK] Target not found'); return; }
        if (target.ws === ws) { console.log('[KICK] Cannot kick self'); return; }
        send(target.ws, { type: 'kicked', reason: 'Kicked by host' });
        var kicked_ws = target.ws;
        room.players = room.players.filter(function(p) { return p.ws !== kicked_ws; });
        peerToRoom.delete(kicked_ws);
        kicked_ws.roomId = null;
        if (room.players.length > 0) {
            broadcastPlayerList(room);
            broadcastRooms();
        } else {
            rooms.delete(ws.roomId);
        }
        console.log('[ROOM] Player ' + target_id + ' kicked');
        setTimeout(function() {
            if (kicked_ws.readyState === WebSocket.OPEN) kicked_ws.close();
        }, 500);

    } else if (type === 'start_game') {
        const room = rooms.get(ws.roomId);
        if (!room || room.host_ws !== ws) return;
        const allReady = room.players.every(function(p) { return p.ready; });
        if (!allReady) { send(ws, { type: 'error', message: 'Not all players are ready' }); return; }
        room.players.forEach(function(p) {
            send(p.ws, { type: 'game_started', host_ip: room.host_ip });
        });
        console.log('[ROOM] Game started in ' + ws.roomId);

    } else if (type === 'leave_room') {
        leaveRoom(ws);

    } else if (type === 'chat') {
        const room = rooms.get(ws.roomId);
        if (!room) return;
        room.players.forEach(function(p) {
            if (p.ws !== ws) send(p.ws, { type: 'chat', from: msg.from, text: msg.text });
        });

    } else {
        console.log('Unknown message:', type);
    }
}

function broadcastPlayerList(room) {
    var playerList = room.players.map(function(p) {
        return { id: p.id, name: p.name, ready: p.ready, ping: p.ping || 0 };
    });
    room.players.forEach(function(p) {
        send(p.ws, { type: 'player_list', players: playerList });
    });
}

function leaveRoom(ws) {
    var roomId = peerToRoom.get(ws);
    if (!roomId) return;
    var room = rooms.get(roomId);
    if (!room) return;
    room.players = room.players.filter(function(p) { return p.ws !== ws; });
    peerToRoom.delete(ws);
    if (room.players.length === 0) {
        rooms.delete(roomId);
        console.log('[ROOM] Deleted: ' + roomId);
    } else {
        if (room.host_ws === ws) {
            room.host_ws = room.players[0].ws;
            room.host_ip = room.players[0].ws._ip || 'host';
            send(room.host_ws, { type: 'you_are_host' });
        }
        broadcastPlayerList(room);
        room.players.forEach(function(p) {
            send(p.ws, { type: 'peer_left', name: ws.playerName || 'Player' });
        });
    }
    broadcastRooms();
}

function broadcastRooms() {
    var list = getRoomList();
    wss.clients.forEach(function(client) {
        if (client.readyState === WebSocket.OPEN && !client.roomId) {
            send(client, { type: 'room_list', rooms: list });
        }
    });
}

function getRoomList() {
    var list = [];
    rooms.forEach(function(room, id) {
        list.push({ id: id, name: room.name, map: room.map || 'de_dust2', players: room.players.length, max: room.max_players });
    });
    return list;
}

function send(ws, data) {
    if (ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify(data));
    }
}

function generateId() {
    return Math.random().toString(36).substring(2, 8);
}

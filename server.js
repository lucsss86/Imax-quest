const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = Number(process.env.PORT || 3000);
const GAME_FILE = path.join(__dirname, 'imax-quest-parkour.html');
const PUBLIC_BASE_URL = String(process.env.PUBLIC_BASE_URL || '').replace(/\/$/, '');
const rooms = new Map();
const connections = new Map();

function roomCode() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let code;
  do { code = Array.from({length:6},()=>chars[crypto.randomInt(chars.length)]).join(''); } while (rooms.has(code));
  return code;
}
function cleanName(value) { return String(value||'Jogador').replace(/[<>]/g,'').trim().slice(0,18)||'Jogador'; }
function getBaseUrl(req) { return PUBLIC_BASE_URL || `${req?.headers?.['x-forwarded-proto']||'http'}://${req?.headers?.host||`localhost:${PORT}`}`; }
function publicPlayer(p) { return {id:p.id,name:p.name,role:p.role,mode:p.mode,x:p.x,correct:p.correct,time:p.time,bossDamage:p.bossDamage,hearts:p.hearts,bossHp:p.bossHp,finished:p.finished}; }
function publicRooms() { return [...rooms.values()].filter(r=>r.public&&r.players.size>0).map(r=>({code:r.code,hostName:r.players.get(r.hostId)?.name||'ADM',players:[...r.players.values()].filter(p=>p.mode==='player').length,createdAt:r.createdAt})); }
function broadcastRoom(room) { const payload=JSON.stringify({type:'snapshot',code:room.code,players:[...room.players.values()].map(publicPlayer)}); for(const p of room.players.values())if(p.ws.readyState===WebSocket.OPEN)p.ws.send(payload); }
function assignHost(room) { if(!(room.hostId&&room.players.has(room.hostId)))room.hostId=[...room.players.values()].find(p=>p.mode==='player')?.id||null;for(const p of room.players.values())p.role=p.id===room.hostId?'host':'player'; }
function removeFromRoom(id) { const conn=connections.get(id);if(!conn?.roomCode)return;const room=rooms.get(conn.roomCode);if(!room)return;room.players.delete(id);if(room.hostId===id)room.hostId=null;assignHost(room);if(room.players.size===0){room.emptyAt=Date.now()}else broadcastRoom(room);conn.roomCode=null; }
function createPlayer(id,ws,name) { return {id,ws,name:cleanName(name),role:'player',mode:'player',x:180,correct:0,time:0,bossDamage:0,hearts:5,bossHp:500,finished:false}; }
function sendWelcome(req,ws,room,player) { const inviteUrl=`${getBaseUrl(req)}/?room=${room.code}`;ws.send(JSON.stringify({type:'welcome',id:player.id,role:player.role,code:room.code,inviteUrl})); }

const server=http.createServer((req,res)=>{
  const url=new URL(req.url,`http://${req.headers.host||'localhost'}`);
  if(url.pathname==='/api/rooms'){res.writeHead(200,{'Content-Type':'application/json','Cache-Control':'no-store'});return res.end(JSON.stringify({rooms:publicRooms()}));}
  if(url.pathname==='/health'){res.writeHead(200,{'Content-Type':'text/plain'});return res.end('ok');}
  if(url.pathname==='/'||url.pathname==='/imax-quest-parkour.html'){return fs.readFile(GAME_FILE,(err,data)=>{if(err){res.writeHead(500);return res.end('Jogo não encontrado.')}res.writeHead(200,{'Content-Type':'text/html; charset=utf-8','Cache-Control':'no-store'});res.end(data)});}
  res.writeHead(404);res.end('Não encontrado');
});

const wss=new WebSocketServer({server,maxPayload:8192});
wss.on('connection',(ws,req)=>{
  const id=crypto.randomUUID();connections.set(id,{ws,roomCode:null});
  ws.on('message',raw=>{
    let msg;try{msg=JSON.parse(String(raw))}catch{return}
    if(msg.type==='createRoom'){
      removeFromRoom(id);const code=roomCode();const room={code,public:true,hostId:id,players:new Map(),createdAt:Date.now(),emptyAt:null};const p=createPlayer(id,ws,msg.name);p.role='host';room.players.set(id,p);rooms.set(code,room);connections.get(id).roomCode=code;sendWelcome(req,ws,room,p);broadcastRoom(room);return;
    }
    if(msg.type==='joinRoom'){
      const code=String(msg.code||'').trim().toUpperCase();const room=rooms.get(code);if(!room){ws.send(JSON.stringify({type:'error',message:'Sala não encontrada. Confira o código ou peça um novo link ao ADM.'}));return}removeFromRoom(id);const p=createPlayer(id,ws,msg.name);room.players.set(id,p);connections.get(id).roomCode=code;assignHost(room);sendWelcome(req,ws,room,p);broadcastRoom(room);return;
    }
    const conn=connections.get(id),room=rooms.get(conn?.roomCode),p=room?.players.get(id);if(!p||!room)return;
    if(msg.type==='switchMode'){if(p.role!=='host'){ws.send(JSON.stringify({type:'error',message:'Somente o ADM pode alternar o modo de espectador.'}));return}p.mode=msg.mode==='spectator'?'spectator':'player';ws.send(JSON.stringify({type:'modeChanged',mode:p.mode}));broadcastRoom(room);return;}
    if(msg.type==='state'&&p.mode==='player'){p.x=Math.max(0,Math.min(13200,Number(msg.x)||0));p.correct=Math.max(p.correct,Math.min(20,Number(msg.correct)||0));p.time=Math.max(0,Number(msg.time)||0);p.bossDamage=Math.max(p.bossDamage,Number(msg.bossDamage)||0);p.hearts=Math.max(0,Math.min(5,Number(msg.hearts)||0));p.bossHp=Math.max(0,Math.min(500,Number(msg.bossHp)||500));p.finished=!!msg.finished;}
    if(msg.type==='answer')p.correct=Math.max(p.correct,Math.min(20,Number(msg.correct)||0));
    if(msg.type==='bossHit')p.bossDamage=Math.max(p.bossDamage,Number(msg.totalDamage)||p.bossDamage+(Number(msg.damage)||0));
    if(msg.type==='finish'){p.correct=Math.min(20,Number(msg.correct)||p.correct);p.time=Number(msg.time)||p.time;p.bossDamage=Number(msg.bossDamage)||p.bossDamage;p.finished=true;}
    broadcastRoom(room);
  });
  ws.on('close',()=>{removeFromRoom(id);connections.delete(id)});
});

setInterval(()=>{for(const room of rooms.values())if(room.players.size)broadcastRoom(room);else if(room.emptyAt&&Date.now()-room.emptyAt>10*60*1000)rooms.delete(room.code)},250);
server.listen(PORT,'0.0.0.0',()=>console.log(`IMAX Quest online na porta ${PORT} — crie salas pelo navegador.`));

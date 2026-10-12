const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = Number(process.env.PORT || 3000);
const GAME_FILE = path.join(__dirname, 'imax-quest-parkour.html');
const PUBLIC_BASE_URL = String(process.env.PUBLIC_BASE_URL || '').replace(/\/$/, '');
const WORLD_WIDTH = 21000;
const TOTAL_QUESTIONS = 10;
const BOSS_MAX_HP = 900;
const PLAYER_COLORS = ['#28e7ff','#ff3e8d','#b8ff36','#ffce45','#8d65ff','#ff6b35','#38e58c','#ff5c73'];
const rooms = new Map();
const connections = new Map();

const bossQuestions = [
  {q:'Qual sequência representa corretamente o funcionamento da câmera apresentado no roteiro?',a:['Luz → lente → sensor → gravação','Sensor → lente → som → tela','Lente → projeção → sensor → gravação','Luz → gravação → lente → sensor'],c:0},
  {q:'Qual é a função que diferencia o sensor do sistema de gravação?',a:['O sensor guarda os arquivos; a gravação cria a luz','O sensor transforma luz em imagem; a gravação armazena os quadros','O sensor controla o som; a gravação define o foco','Ambos exercem a mesma função'],c:1},
  {q:'Qual taxa de quadros foi citada como normalmente utilizada no cinema?',a:['18 fps','24 fps','30 fps','60 fps'],c:1},
  {q:'Qual combinação localiza corretamente o surgimento do IMAX?',a:['Japão, início dos anos 1950','Canadá, final dos anos 1960','Estados Unidos, final dos anos 1980','Canadá, início dos anos 2000'],c:1},
  {q:'Qual foi o primeiro grande marco de apresentação do IMAX?',a:['Expo 70, no Japão, em 1970','Expo 67, no Canadá, em 1967','Festival de Cannes, em 1970','Feira de Hollywood, em 1980'],c:0},
  {q:'Por que documentários e filmes educativos combinaram com o IMAX no início?',a:['Porque não precisavam de projeção','Porque as telas enormes valorizavam paisagens, natureza e ciência','Porque usavam somente imagens quadradas','Porque eram sempre gravados sem som'],c:1},
  {q:'O que se pode concluir sobre filmes exibidos em uma sala IMAX?',a:['Todos foram filmados inteiros com câmera IMAX','Nenhum utiliza câmera tradicional','Podem utilizar câmeras IMAX ou tradicionais destinadas ao formato','Sempre mostram mais conteúdo vertical'],c:2},
  {q:'Quando o IMAX pode mostrar mais conteúdo verticalmente?',a:['Em qualquer filme e sala','Dependendo da sala e do filme','Somente em documentários','Apenas quando não há som'],c:1},
  {q:'Qual fórmula resume o IMAX como um sistema completo?',a:['Captura + projeção + tela + som','Lente + ingresso + cadeira + edição','Sensor + tela + preço + pipoca','Câmera + legenda + iluminação'],c:0},
  {q:'Qual comparação de disponibilidade e preço está correta?',a:['IMAX costuma ter mais salas e menor preço','Tradicional costuma ter mais salas e menor preço','Ambos sempre custam o mesmo','Tradicional possui menos salas e maior preço'],c:1},
  {q:'Qual característica foi atribuída à sala IMAX?',a:['Tela menor e som convencional','Tela maior, projeção específica e som mais envolvente','Ausência de sistema de projeção','Uso obrigatório de película em todo filme'],c:1},
  {q:'O que a lente determina além de receber a luz da cena?',a:['Preço e duração','Foco e enquadramento','Som e projeção','Armazenamento e edição'],c:1},
  {q:'Qual mudança ocorreu conforme a tecnologia IMAX evoluiu?',a:['Ficou restrita às escolas','Passou a integrar também grandes produções cinematográficas','Abandonou as telas maiores','Deixou de utilizar sistemas de som'],c:1},
  {q:'Qual é o objetivo das tecnologias IMAX trabalhando juntas?',a:['Diminuir a duração do filme','Aumentar a sensação de imersão','Eliminar a necessidade de captura','Tornar todas as salas idênticas'],c:1},
  {q:'Qual conclusão está de acordo com a apresentação?',a:['A diferença do IMAX está apenas no tamanho da tela','IMAX e cinema tradicional não usam som','Imagem, projeção, tela e som trabalham juntos para contar histórias','Todo filme aproveita igualmente todos os recursos do IMAX'],c:2}
];

function roomCode() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let code;
  do { code = Array.from({length:6},()=>chars[crypto.randomInt(chars.length)]).join(''); } while (rooms.has(code));
  return code;
}
function cleanName(value) { return String(value||'Jogador').replace(/[<>]/g,'').trim().slice(0,18)||'Jogador'; }
function getBaseUrl(req) { return PUBLIC_BASE_URL || `${req?.headers?.['x-forwarded-proto']||'http'}://${req?.headers?.host||`localhost:${PORT}`}`; }
function publicPlayer(p,room) { return {id:p.id,name:p.name,role:p.role,mode:p.mode,color:p.color,x:p.x,correct:p.correct,time:p.time,bossDamage:p.bossDamage,hearts:p.hearts,bossHp:room.bossHp,finished:p.finished,bossReady:p.bossReady}; }
function publicRooms() { return [...rooms.values()].filter(r=>r.public&&r.players.size>0).map(r=>({code:r.code,hostName:r.players.get(r.hostId)?.name||'ADM',players:[...r.players.values()].filter(p=>p.mode==='player').length,phase:r.phase,createdAt:r.createdAt})); }
function sendToRoom(room,payload,filter=()=>true) { const data=JSON.stringify(payload);for(const p of room.players.values())if(filter(p)&&p.ws.readyState===WebSocket.OPEN)p.ws.send(data); }
function broadcastRoom(room) { sendToRoom(room,{type:'snapshot',code:room.code,phase:room.phase,startAt:room.startAt,bossHp:room.bossHp,players:[...room.players.values()].map(p=>publicPlayer(p,room))}); }
function broadcastBossHealth(room,source='sync') { sendToRoom(room,{type:'bossHealth',hp:room.bossHp,maxHp:BOSS_MAX_HP,source});broadcastRoom(room); }
function assignHost(room) { const previousHost=room.hostId;if(!(room.hostId&&room.players.has(room.hostId)))room.hostId=[...room.players.values()].find(p=>p.mode==='player')?.id||null;if(room.hostId&&room.hostId!==previousHost)room.hostToken=crypto.randomBytes(24).toString('hex');for(const p of room.players.values()){p.role=p.id===room.hostId?'host':'player';if(p.ws.readyState===WebSocket.OPEN)p.ws.send(JSON.stringify({type:'roleUpdate',role:p.role,hostToken:p.role==='host'?room.hostToken:undefined}))} }
function removeFromRoom(id) { const conn=connections.get(id);if(!conn?.roomCode)return;const room=rooms.get(conn.roomCode);if(!room)return;room.players.delete(id);if(room.hostId===id)room.hostId=null;assignHost(room);if(room.players.size===0){room.emptyAt=Date.now()}else broadcastRoom(room);conn.roomCode=null; }
function createPlayer(id,ws,name,color) { return {id,ws,name:cleanName(name),color,role:'player',mode:'player',x:180,correct:0,time:0,bossDamage:0,hearts:5,finished:false,bossReady:false}; }
function sendWelcome(req,ws,room,player) { const inviteUrl=`${getBaseUrl(req)}/?room=${room.code}`;ws.send(JSON.stringify({type:'welcome',id:player.id,role:player.role,color:player.color,code:room.code,inviteUrl,bossHp:room.bossHp,maxHp:BOSS_MAX_HP,phase:room.phase,startAt:room.startAt,hostToken:player.role==='host'?room.hostToken:undefined})); }
function resetPlayer(p) { p.mode='player';p.x=180;p.correct=0;p.time=0;p.bossDamage=0;p.hearts=5;p.finished=false;p.bossReady=false; }
function startRoom(room) { if(room.phase!=='countdown')return;room.phase='running';sendToRoom(room,{type:'matchStart',startedAt:room.startAt});broadcastRoom(room); }
function beginCountdown(room) { room.phase='countdown';room.startAt=Date.now()+3000;room.bossHp=BOSS_MAX_HP;room.bossRound=null;room.nextBossQuizAt=null;room.quizNumber=0;for(const p of room.players.values())resetPlayer(p);clearTimeout(room.startTimer);room.startTimer=setTimeout(()=>startRoom(room),Math.max(0,room.startAt-Date.now()));sendToRoom(room,{type:'matchCountdown',startAt:room.startAt});broadcastRoom(room); }

function bossParticipants(room) { return [...room.players.values()].filter(p=>p.mode==='player'&&p.bossReady&&!p.finished); }
function startBossRound(room,now) {
  const eligible=bossParticipants(room);if(!eligible.length||room.bossHp<=0)return;
  const questionIndex=room.quizNumber%bossQuestions.length,q=bossQuestions[questionIndex];room.quizNumber++;
  room.bossRound={id:`${room.code}-${room.quizNumber}-${now}`,questionIndex,startedAt:now,deadline:now+7000,eligible:new Set(eligible.map(p=>p.id)),answers:new Map()};
  const payload={type:'bossQuestion',roundId:room.bossRound.id,number:room.quizNumber,question:q.q,answers:q.a,deadline:room.bossRound.deadline,requiredPercent:30,damage:35};
  sendToRoom(room,payload,p=>room.bossRound.eligible.has(p.id));room.nextBossQuizAt=now+10000;
}
function resolveBossRound(room) {
  const round=room.bossRound;if(!round)return;const q=bossQuestions[round.questionIndex];
  const eligible=[...round.eligible].filter(id=>{const p=room.players.get(id);return p&&p.mode==='player'&&p.bossReady&&!p.finished});
  let correct=0;for(const id of eligible)if(round.answers.get(id)===q.c)correct++;
  const ratio=eligible.length?correct/eligible.length:0,success=ratio>.30;
  if(success)room.bossHp=Math.max(0,room.bossHp-35);
  const payload={type:'bossQuizResult',roundId:round.id,correctAnswer:q.c,correct,total:eligible.length,percent:Math.round(ratio*100),success,damage:success?35:0,bossHp:room.bossHp};
  sendToRoom(room,payload,p=>p.bossReady);room.bossRound=null;broadcastBossHealth(room,success?'teamQuiz':'quizMiss');
}

const server=http.createServer((req,res)=>{
  const url=new URL(req.url,`http://${req.headers.host||'localhost'}`);
  if(url.pathname==='/api/rooms'){res.writeHead(200,{'Content-Type':'application/json','Cache-Control':'no-store'});return res.end(JSON.stringify({rooms:publicRooms()}));}
  if(url.pathname==='/health'){res.writeHead(200,{'Content-Type':'text/plain'});return res.end('ok');}
  if(url.pathname==='/api/start'&&req.method==='POST'){
    let body='';req.on('data',chunk=>{body+=chunk;if(body.length>4096)req.destroy()});req.on('end',()=>{
      let data;try{data=JSON.parse(body||'{}')}catch{res.writeHead(400,{'Content-Type':'application/json'});return res.end(JSON.stringify({ok:false,error:'Pedido inválido.'}))}
      const code=String(data.code||'').trim().toUpperCase(),room=rooms.get(code);
      if(!room||!data.hostToken||data.hostToken!==room.hostToken){res.writeHead(403,{'Content-Type':'application/json','Cache-Control':'no-store'});return res.end(JSON.stringify({ok:false,error:'Confirmação do ADM inválida.'}))}
      if(room.phase==='lobby'||room.phase==='finished')beginCountdown(room);
      res.writeHead(200,{'Content-Type':'application/json','Cache-Control':'no-store'});res.end(JSON.stringify({ok:true,phase:room.phase,startAt:room.startAt}));
    });return;
  }
  if(url.pathname==='/'||url.pathname==='/imax-quest-parkour.html'){return fs.readFile(GAME_FILE,(err,data)=>{if(err){res.writeHead(500);return res.end('Jogo não encontrado.')}res.writeHead(200,{'Content-Type':'text/html; charset=utf-8','Cache-Control':'no-store'});res.end(data)});}
  res.writeHead(404);res.end('Não encontrado');
});

const wss=new WebSocketServer({server,maxPayload:8192});
wss.on('connection',(ws,req)=>{
  const id=crypto.randomUUID();connections.set(id,{ws,roomCode:null});
  ws.on('message',raw=>{
    let msg;try{msg=JSON.parse(String(raw))}catch{return}
    if(msg.type==='createRoom'){
      removeFromRoom(id);const code=roomCode();const room={code,public:true,hostId:id,hostToken:crypto.randomBytes(24).toString('hex'),players:new Map(),createdAt:Date.now(),emptyAt:null,phase:'lobby',startAt:null,bossHp:BOSS_MAX_HP,bossRound:null,nextBossQuizAt:null,quizNumber:0};const p=createPlayer(id,ws,msg.name,PLAYER_COLORS[0]);p.role='host';room.players.set(id,p);rooms.set(code,room);connections.get(id).roomCode=code;sendWelcome(req,ws,room,p);broadcastRoom(room);return;
    }
    if(msg.type==='joinRoom'){
      const code=String(msg.code||'').trim().toUpperCase();const room=rooms.get(code);if(!room){ws.send(JSON.stringify({type:'error',message:'Sala não encontrada. Confira o código ou peça um novo link ao ADM.'}));return}removeFromRoom(id);const used=new Set([...room.players.values()].map(p=>p.color));const color=PLAYER_COLORS.find(c=>!used.has(c))||PLAYER_COLORS[room.players.size%PLAYER_COLORS.length];const p=createPlayer(id,ws,msg.name,color);room.players.set(id,p);connections.get(id).roomCode=code;assignHost(room);sendWelcome(req,ws,room,p);broadcastRoom(room);return;
    }
    const conn=connections.get(id),room=rooms.get(conn?.roomCode),p=room?.players.get(id);if(!p||!room)return;
    if(msg.type==='startMatch'){
      if(p.role!=='host'){ws.send(JSON.stringify({type:'error',message:'Somente o ADM pode iniciar a partida.'}));return}
      // O ADM conta como dono da sala e pode iniciar sozinho, sem convidados.
      // Não há quantidade mínima ou máxima lógica de jogadores para começar.
      if(room.phase==='lobby'||room.phase==='finished'){beginCountdown(room);return}
      // Respostas idempotentes evitam que um clique repetido ou uma reconexão
      // deixe o botão parecendo travado no navegador do ADM.
      if(room.phase==='countdown'){ws.send(JSON.stringify({type:'matchCountdown',startAt:room.startAt}));return}
      if(room.phase==='running'){ws.send(JSON.stringify({type:'matchStart',startedAt:room.startAt}));return}
    }
    if(msg.type==='switchMode'){if(p.role!=='host'){ws.send(JSON.stringify({type:'error',message:'Somente o ADM pode alternar o modo de espectador.'}));return}p.mode=msg.mode==='spectator'?'spectator':'player';ws.send(JSON.stringify({type:'modeChanged',mode:p.mode}));broadcastRoom(room);return;}
    if(msg.type==='state'&&room.phase==='running'&&p.mode==='player'){p.x=Math.max(0,Math.min(WORLD_WIDTH,Number(msg.x)||0));p.correct=Math.max(p.correct,Math.min(TOTAL_QUESTIONS,Number(msg.correct)||0));p.time=Math.max(0,Number(msg.time)||0);p.hearts=Math.max(0,Math.min(5,Number(msg.hearts)||0));p.finished=!!msg.finished;}
    if(msg.type==='answer'&&room.phase==='running')p.correct=Math.max(p.correct,Math.min(TOTAL_QUESTIONS,Number(msg.correct)||0));
    if(msg.type==='bossReady'&&room.phase==='running'&&p.mode==='player'){p.bossReady=true;if(room.nextBossQuizAt===null)room.nextBossQuizAt=Date.now()+10000;ws.send(JSON.stringify({type:'bossHealth',hp:room.bossHp,maxHp:BOSS_MAX_HP,source:'ready'}));}
    if(msg.type==='bossQuizAnswer'&&room.bossRound&&room.bossRound.id===msg.roundId&&room.bossRound.eligible.has(id)&&!room.bossRound.answers.has(id)){room.bossRound.answers.set(id,Math.max(0,Math.min(3,Number(msg.choice)||0)));}
    if(msg.type==='bossHit'&&p.bossReady&&room.bossHp>0){const damage=Math.max(0,Math.min(10,Number(msg.damage)||0));if(damage){room.bossHp=Math.max(0,room.bossHp-damage);p.bossDamage+=damage;broadcastBossHealth(room,'jump');}}
    if(msg.type==='finish'&&room.phase==='running'){p.correct=Math.min(TOTAL_QUESTIONS,Number(msg.correct)||p.correct);p.time=Number(msg.time)||p.time;p.bossDamage=Number(msg.bossDamage)||p.bossDamage;p.finished=true;if([...room.players.values()].filter(x=>x.mode==='player').every(x=>x.finished))room.phase='finished';}
    if(msg.type!=='state')broadcastRoom(room);
  });
  ws.on('close',()=>{removeFromRoom(id);connections.delete(id)});
});

setInterval(()=>{
  const now=Date.now();
  for(const room of rooms.values()){
    if(room.players.size){
      if(room.phase==='countdown'&&now>=room.startAt)startRoom(room);
      if(room.phase==='running'&&room.bossRound&&now>=room.bossRound.deadline)resolveBossRound(room);
      if(room.phase==='running'&&!room.bossRound&&room.nextBossQuizAt!==null&&now>=room.nextBossQuizAt&&room.bossHp>0)startBossRound(room,now);
      broadcastRoom(room);
    }else if(room.emptyAt&&now-room.emptyAt>10*60*1000)rooms.delete(room.code);
  }
},500);
server.listen(PORT,'0.0.0.0',()=>console.log(`IMAX Quest online na porta ${PORT} — crie salas pelo navegador.`));

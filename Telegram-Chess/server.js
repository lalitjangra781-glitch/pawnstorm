'use strict';
/*  Run:  BOT_TOKEN=123:ABC node server.js      (local test without Telegram: DEV=1 node server.js)  */
const http=require('http'),fs=require('fs'),path=require('path'),crypto=require('crypto');
const {WebSocketServer}=require('ws');
const E=require('./engine');
const TOKEN=process.env.BOT_TOKEN||'',PORT=+process.env.PORT||3000,DEV=process.env.DEV==='1';

/* ---------- storage (JSON file; swap with a real DB later) ---------- */
const DBF=path.join(__dirname,'ratings.json');
/* If DATABASE_URL is set -> ratings live in a free Postgres (Neon). Otherwise a local ratings.json file is used. */
let users={},pool=null;const dirty=new Set(),touch=id=>dirty.add(String(id));
async function loadDB(){
 if(process.env.DATABASE_URL){const{Pool}=require('pg');
  pool=new Pool({connectionString:process.env.DATABASE_URL,ssl:{rejectUnauthorized:false},max:3});pool.on('error',e=>console.log('db error:',e.message));
  await pool.query('create table if not exists players(id text primary key, data jsonb not null)');
  const r=await pool.query('select id,data from players');for(const x of r.rows)users[x.id]=x.data;
  console.log('Database connected, players loaded:',r.rows.length)}
 else{try{users=JSON.parse(fs.readFileSync(DBF,'utf8'))}catch(e){}console.log('No DATABASE_URL: using ratings.json (data is lost on free hosts when they restart!)')}}
async function flush(){if(!dirty.size)return;const ids=[...dirty];dirty.clear();
 try{if(pool){for(const id of ids)await pool.query('insert into players(id,data) values($1,$2) on conflict(id) do update set data=$2',[id,users[id]])}
  else fs.writeFileSync(DBF,JSON.stringify(users))}
 catch(e){console.log('save failed:',e.message);ids.forEach(i=>dirty.add(i))}}
setInterval(flush,5000);
process.on('SIGTERM',async()=>{await flush();process.exit(0)});
const rec=id=>users[id]||(users[id]={name:'',bullet:800,blitz:800,rapid:800,w:0,l:0,d:0});
const pub=u=>({bullet:u.bullet,blitz:u.blitz,rapid:u.rapid,w:u.w,l:u.l,d:u.d});
const cat=(m,i)=>{const x=m+i*40/60;return x<3?'bullet':x<10?'blitz':'rapid'};

/* ---------- Telegram initData verification ---------- */
function verify(init){if(!init)return null;const p=new URLSearchParams(init),h=p.get('hash');if(!h)return null;p.delete('hash');
 const s=[...p.entries()].map(([k,v])=>k+'='+v).sort().join('\n');
 const sk=crypto.createHmac('sha256','WebAppData').update(TOKEN).digest();
 const c=crypto.createHmac('sha256',sk).update(s).digest('hex');
 if(c!==h)return null;if(Date.now()/1000-(+p.get('auth_date')||0)>86400)return null;
 try{return JSON.parse(p.get('user'))}catch(e){return null}}

/* ---------- http + ws ---------- */
const server=http.createServer((req,res)=>{
 if(req.url==='/health'){res.writeHead(200,{'Content-Type':'application/json'});res.end(JSON.stringify({ok:true,bot:BOT_USER||null,polling:DBG.polling,app_link:APP_LINK||null,inline_queries_received:DBG.inline,inline_answered_ok:DBG.answered,last_inline_query:DBG.last,last_error:DBG.err,database:!!pool},null,1));return}
 if(req.url.split('?')[0]==='/')fs.readFile(path.join(__dirname,'index.html'),(e,d)=>{res.writeHead(e?500:200,{'Content-Type':'text/html; charset=utf-8'});res.end(e?'error':d)});
 else{res.writeHead(404);res.end()}});
const wss=new WebSocketServer({server,maxPayload:2048});
const conns=new Map(),inGame=new Map();let queue=[];
const send=(uid,o)=>{const w=conns.get(uid);if(w&&w.readyState===1)w.send(JSON.stringify(o))};

wss.on('connection',ws=>{
 ws.on('message',raw=>{let m;try{m=JSON.parse(raw)}catch(e){return}
  if(m.t==='auth'){
   const u=(TOKEN&&verify(m.initData))||(DEV&&{id:'dev_'+String(m.name||'guest').slice(0,20),first_name:String(m.name||'Guest').slice(0,20)});
   if(!u){ws.send(JSON.stringify({t:'err',msg:'auth failed'}));return ws.close()}
   const uid=String(u.id),old=conns.get(uid);if(old&&old!==ws){old.uid=null;old.close()}
   ws.uid=uid;conns.set(uid,ws);const r=rec(uid);r.name=((u.first_name||'')+(u.last_name?' '+u.last_name:'')).slice(0,30)||'Player';touch(uid);
   send(uid,{t:'me',r:pub(r),bot:BOT_USER,app:APP_LINK});
   const g=inGame.get(uid);if(g&&!g.over){clearTimeout(g.dc[g.pl.w===uid?'w':'b']);sendStart(g,uid)}
   return}
  const uid=ws.uid;if(!uid)return;
  if(m.t==='queue')joinQueue(uid,+m.m,+m.i);
  else if(m.t==='cancel'){queue=queue.filter(q=>q.uid!==uid);for(const r of rooms.values())if(r.host===uid&&!r.used)r.host=null}
  else if(m.t==='join')joinRoom(uid,String(m.room||''));
  else if(m.t==='move')onMove(uid,m);
  else if(m.t==='resign'){const g=inGame.get(uid);if(g)end(g,g.pl.w===uid?'b':'w','Resigned')}
  else if(m.t==='lb')send(uid,Object.assign({t:'lb'},lb()))});
 ws.on('close',()=>{const uid=ws.uid;if(!uid||conns.get(uid)!==ws)return;conns.delete(uid);queue=queue.filter(q=>q.uid!==uid);
  const g=inGame.get(uid);if(g&&!g.over){const col=g.pl.w===uid?'w':'b';
   g.dc[col]=setTimeout(()=>{if(!g.over&&!conns.has(uid))end(g,col==='w'?'b':'w','Disconnected')},60000)}})});

/* ---------- matchmaking: same time control, rating window grows with waiting time ---------- */
function joinQueue(uid,m,i){const cur=inGame.get(uid);if(cur&&!cur.over)return sendStart(cur,uid);
 if(!(Number.isInteger(m)&&Number.isInteger(i)&&m>=1&&m<=180&&i>=0&&i<=60))return;
 queue=queue.filter(q=>q.uid!==uid);queue.push({uid,m,i,r:users[uid][cat(m,i)],since:Date.now()});send(uid,{t:'queued'});match()}
const win=(q,now)=>Math.min(100+(now-q.since)/1000*25,1000);
function match(){const now=Date.now();
 for(const a of queue){let best=null,bd=1e9;
  for(const b of queue){if(a===b||a.m!==b.m||a.i!==b.i)continue;const d=Math.abs(a.r-b.r);
   if(d<=Math.max(win(a,now),win(b,now))&&d<bd){bd=d;best=b}}
  if(best){queue=queue.filter(q=>q!==a&&q!==best);startGame(a,best);return match()}}}
setInterval(match,1000);

/* ---------- Telegram bot: inline invites (@bot in any chat/group) + friendly rooms ---------- */
const APP_LINK=(process.env.APP_LINK||'').replace(/\/$/,'');   /* e.g. https://t.me/mychessbot/play  (the Mini App direct link) */
const DBG={inline:0,answered:0,last:null,err:null,polling:false};
let BOT_USER='';const rooms=new Map(),sleep=ms=>new Promise(r=>setTimeout(r,ms));
const esc=s=>String(s).replace(/[&<>]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]));
const tgcall=(m,b)=>fetch(`https://api.telegram.org/bot${TOKEN}/${m}`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(b||{})}).then(r=>r.json());
const PRESETS=[[5,0],[3,0],[3,2],[1,0],[10,0],[15,10]];
function inlineQuery(q){const list=PRESETS.slice(),mm=/^\s*(\d{1,3})\s*\+\s*(\d{1,2})\s*$/.exec(q.query||'');
 if(mm&&+mm[1]>=1&&+mm[1]<=180&&+mm[2]<=60)list.unshift([+mm[1],+mm[2]]);   /* "@bot 7+5" = custom time */
 const nm=esc(q.from.first_name||'Player'),seen=new Set();
 const results=list.filter(([m,i])=>!seen.has(m+'+'+i)&&seen.add(m+'+'+i)).map(([m,i])=>{
  const id=`m${m}i${i}_${crypto.randomBytes(4).toString('hex')}`,c=cat(m,i);
  return{type:'article',id,title:`♟ Chess ${m}+${i} · ${c}`,description:'Challenge this chat to a game',
   input_message_content:{message_text:`♟ <b>Chess challenge</b> · ${m}+${i} ${c}\n${nm} wants to play. Tap <b>Play</b> – the first person to join gets the game!`,parse_mode:'HTML'},
   reply_markup:{inline_keyboard:[[{text:'♟ Play',url:`${APP_LINK}?startapp=${id}`}]]}}});
 tgcall('answerInlineQuery',{inline_query_id:q.id,results,cache_time:0,is_personal:true})
  .then(r=>{if(r.ok)DBG.answered++;else{DBG.err='answerInlineQuery: '+r.description;console.log(DBG.err)}}).catch(e=>{DBG.err='answerInlineQuery: '+e.message;console.log(DBG.err)})}
function getRoom(id){const mm=/^m(\d{1,3})i(\d{1,2})_[a-f0-9]{8}$/.exec(id);if(!mm)return null;const m=+mm[1],i=+mm[2];if(m<1||m>180||i>60)return null;
 let r=rooms.get(id);if(!r){r={id,m,i,host:null,used:false,t:Date.now(),inline:null};rooms.set(id,r)}return r}
function joinRoom(uid,id){const cur=inGame.get(uid);if(cur&&!cur.over)return sendStart(cur,uid);
 const r=getRoom(id);if(!r)return send(uid,{t:'err',msg:'Invalid invite link'});
 if(r.used)return send(uid,{t:'err',msg:'This game is already full'});
 queue=queue.filter(q=>q.uid!==uid);
 if(r.host&&r.host!==uid&&conns.has(r.host)){r.used=true;const h=r.host;
  startGame({uid:h,m:r.m,i:r.i},{uid,m:r.m,i:r.i},true);
  if(r.inline)tgcall('editMessageText',{inline_message_id:r.inline,parse_mode:'HTML',text:`♟ <b>${esc(users[h].name)}</b> vs <b>${esc(users[uid].name)}</b> · ${r.m}+${r.i}\nGame in progress…`}).catch(()=>{})}
 else{r.host=uid;send(uid,{t:'waiting',m:r.m,i:r.i,room:r.id})}}
setInterval(()=>{for(const[k,r]of rooms)if(Date.now()-r.t>2*3600e3)rooms.delete(k)},600000);
async function poll(){if(!TOKEN)return console.log('No BOT_TOKEN: Telegram invites disabled');
 const me=await tgcall('getMe');if(!me.ok){DBG.err='getMe: '+me.description+' (BOT_TOKEN wrong?)';console.log(DBG.err);return}BOT_USER=me.result.username;DBG.polling=true;
 console.log('Bot @'+BOT_USER+(APP_LINK?'':'  -> set APP_LINK (https://t.me/<bot>/<app>) to enable invites'));
 await tgcall('deleteWebhook').catch(()=>{});let off=0;
 for(;;){try{const r=await tgcall('getUpdates',{offset:off,timeout:30,allowed_updates:['inline_query','chosen_inline_result']});
  if(!r.ok){DBG.err='getUpdates: '+r.description;console.log(DBG.err);await sleep(3000);continue}
  for(const u of r.result){off=u.update_id+1;
   if(u.inline_query){DBG.inline++;DBG.last=new Date().toISOString();if(APP_LINK)inlineQuery(u.inline_query);else{DBG.err='APP_LINK is not set on the server';console.log(DBG.err)}}
   if(u.chosen_inline_result){const r2=getRoom(u.chosen_inline_result.result_id);if(r2&&u.chosen_inline_result.inline_message_id)r2.inline=u.chosen_inline_result.inline_message_id}}}
  catch(e){await sleep(3000)}}}
poll();

/* ---------- games ---------- */
function startGame(a,b,fr){const[w,k]=Math.random()<.5?[a,b]:[b,a],ms=a.m*60000;
 const g={id:crypto.randomUUID(),pl:{w:w.uid,b:k.uid},pos:E.start(),moves:[],clk:{w:ms,b:ms},inc:a.i*1000,m:a.m,i:a.i,cat:cat(a.m,a.i),last:Date.now(),over:false,fr:!!fr,reps:{},timer:null,dc:{}};
 inGame.set(w.uid,g);inGame.set(k.uid,g);sendStart(g,w.uid);sendStart(g,k.uid)}
function clocks(g){const c={w:g.clk.w,b:g.clk.b};if(!g.over&&g.moves.length>=2)c[g.pos.t]=Math.max(0,c[g.pos.t]-(Date.now()-g.last));return c}
function sendStart(g,uid){const col=g.pl.w===uid?'w':'b',oid=g.pl[col==='w'?'b':'w'];
 send(uid,{t:'start',id:g.id,color:col,m:g.m,i:g.i,cat:g.cat,clk:clocks(g),moves:g.moves,me:users[uid][g.cat],opp:{name:users[oid].name,r:users[oid][g.cat]}})}
function arm(g){clearTimeout(g.timer);if(g.over||g.moves.length<2)return;const t=g.pos.t,left=g.clk[t]-(Date.now()-g.last);
 g.timer=setTimeout(()=>end(g,t==='w'?'b':'w','Time out'),Math.max(0,left)+30)}

function onMove(uid,m){const g=inGame.get(uid);if(!g||g.over)return;const col=g.pl.w===uid?'w':'b';
 if(g.pos.t!==col)return sendStart(g,uid);
 const now=Date.now();
 if(g.moves.length>=2){g.clk[col]-=now-g.last;if(g.clk[col]<=0){g.clk[col]=0;return end(g,col==='w'?'b':'w','Time out')}}
 const ms=E.legal(g.pos).filter(x=>x.f===m.f&&x.t===m.to&&(x.p||'')===(m.p||''));
 if(ms.length!==1){if(g.moves.length>=2)g.clk[col]+=now-g.last;return sendStart(g,uid)}   /* illegal -> resync client */
 const x=ms[0];if(g.moves.length>=2)g.clk[col]+=g.inc;
 g.pos=E.make(g.pos,x);g.moves.push({f:x.f,t:x.t,p:x.p});g.last=now;
 const clk={w:g.clk.w,b:g.clk.b},opp=g.pl[col==='w'?'b':'w'];
 send(uid,{t:'ack',clk});send(opp,{t:'move',f:x.f,to:x.t,p:x.p,clk});
 const N=g.pos,k=N.b.join('')+N.t+N.c+N.e;g.reps[k]=(g.reps[k]||0)+1;
 if(!E.legal(N).length){const c=E.inCheck(N);return end(g,c?col:'d',c?'Checkmate':'Stalemate')}
 if(N.h>=100)return end(g,'d','50-move rule');
 if(g.reps[k]>=3)return end(g,'d','Repetition');
 if(E.lowMat(N.b))return end(g,'d','Insufficient material');
 arm(g)}

function end(g,res,why){if(g.over)return;
 if(g.moves.length>=2)g.clk[g.pos.t]=Math.max(0,g.clk[g.pos.t]-(Date.now()-g.last));
 g.over=true;clearTimeout(g.timer);clearTimeout(g.dc.w);clearTimeout(g.dc.b);
 const ab=g.moves.length<2,d={w:0,b:0};
 if(!ab&&!g.fr){const rw=users[g.pl.w][g.cat],rb=users[g.pl.b][g.cat],ew=1/(1+Math.pow(10,(rb-rw)/400)),sw=res==='w'?1:res==='b'?0:.5;
  d.w=Math.round(32*(sw-ew));d.b=-d.w;
  for(const c of['w','b']){const u=users[g.pl[c]];u[g.cat]=Math.max(100,u[g.cat]+d[c]);u[res==='d'?'d':res===c?'w':'l']++}touch(g.pl.w);touch(g.pl.b)}
 for(const c of['w','b']){const uid=g.pl[c];
  send(uid,{t:'over',res,why:ab?'Game aborted':why,ab,fr:g.fr,d:d[c],r:pub(users[uid]),clk:g.clk});inGame.delete(uid)}}

function lb(){const o={};for(const c of['bullet','blitz','rapid'])o[c]=Object.values(users).filter(u=>u.w+u.l+u.d>0).sort((a,b)=>b[c]-a[c]).slice(0,10).map(u=>({name:u.name,r:u[c]}));return o}

loadDB().then(()=>server.listen(PORT,()=>console.log('Chess server on :'+PORT+(DEV?' (DEV mode)':'')))).catch(e=>{console.log('Startup failed:',e.message);process.exit(1)});

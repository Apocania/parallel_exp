"use strict";
const $ = id => document.getElementById(id);
const escapeHtml = value => String(value ?? "").replace(/[&<>"']/g, ch => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[ch]));
const money = cents => "¥" + (cents / 100).toFixed(2);
const uid = () => crypto.randomUUID();
const pause = ms => new Promise(resolve => setTimeout(resolve, ms));
const htmlCache = new WeakMap();
function html(id, value) {
  const element = $(id);
  if (htmlCache.get(element) !== value) { element.innerHTML = value; htmlCache.set(element, value); }
}
let state = null, refreshing = null, refreshAgain = false, refreshTimer, toastTimer;
let mutation = 0, observedMutation = -1, epoch = 0, streamLive = false;
let scene = CartLabScenes[0], stepIndex = 0, busy = false, sceneError = "";
let context = {}, evidence = [];
let messagesData=null, messageSelection="follow", messagesRefreshing=null, messagesTimer, messagesAgain=false;
let lessonId="normal";
const pending = {phone:null, desktop:null}, inFlight = {phone:false, desktop:false};
const feedback = {phone:"可直接添加商品，观察另一端。", desktop:"可直接添加商品，观察另一端。"};
const relationshipNames = {equal:"版本相同", before:"因果先于", after:"因果晚于", concurrent:"并发版本"};
const getNode = (s,id) => s?.nodes.find(n => n.node === id);
const qty = (s,id,sku) => getNode(s,id)?.cart?.items.find(i=>i.sku===sku)?.quantity || 0;
const assert = (condition,message) => { if (!condition) throw new Error(message); };

async function api(path, body) {
  if (body !== undefined) mutation++;
  try {
    const response = await fetch(path, body === undefined ? {cache:"no-store"} : {
      method:"POST", headers:{"Content-Type":"application/json"}, body:JSON.stringify(body)
    });
    const value = await response.json();
    if (!response.ok) {
      const error = new Error(value.error || `HTTP ${response.status}`);
      error.payload = value; error.status = response.status; throw error;
    }
    return value;
  } finally {
    if (body !== undefined) { mutation++; scheduleRefresh(); }
  }
}
function toast(message,error=false) {
  $("toast").textContent = message; $("toast").className = "show" + (error ? " error" : "");
  clearTimeout(toastTimer); toastTimer = setTimeout(()=>$("toast").className="",error?6500:3500);
}
function scheduleRefresh() {
  if (refreshTimer) return;
  refreshTimer = setTimeout(()=>{ refreshTimer=null; refresh(); },40);
}
function refresh() {
  if (refreshing) { refreshAgain=true; return refreshing; }
  const startedMutation=mutation, startedEpoch=epoch;
  refreshing=(async()=>{
    try {
      const observed=await api("/api/status");
      if (startedMutation!==mutation || startedEpoch!==epoch) { refreshAgain=true; return; }
      state=observed; observedMutation=startedMutation;
      acceptMessages(observed);
      $("connection").textContent=streamLive?"实时通知已连接":"正在重连 · 定时读取";
      $("connection").className="live-label";
      render();
    } catch(error) {
      observedMutation=-1;
      $("connection").textContent="控制台连接中断"; $("connection").className="live-label error";
    } finally {
      refreshing=null;
      if (refreshAgain) { refreshAgain=false; scheduleRefresh(); }
    }
    return state;
  })();
  return refreshing;
}
function acceptMessages(data) {
  if(!state || data.cart_id!==state.cart_id) return;
  if(!messagesData || messagesData.cart_id!==data.cart_id ||
     data.message_revision>=messagesData.message_revision) messagesData=data;
}
function scheduleMessages() {
  if(messagesTimer) return;
  messagesTimer=setTimeout(()=>{messagesTimer=null;refreshMessages();},120);
}
async function refreshMessages() {
  if(!state) return;
  if(messagesRefreshing) {messagesAgain=true;return messagesRefreshing;}
  const cart=state.cart_id, startedEpoch=epoch;
  messagesRefreshing=(async()=>{
    try {
      const data=await api("/api/messages?cart_id="+encodeURIComponent(cart));
      if(startedEpoch!==epoch || state.cart_id!==cart) return;
      acceptMessages(data);
      state.network=data.network;
      renderNetwork();renderMessages();
    } catch(_) { /* Full status fallback reports connection failures. */ }
    finally {messagesRefreshing=null;if(messagesAgain){messagesAgain=false;scheduleMessages();}}
  })();
  return messagesRefreshing;
}
async function read() {
  // A read that began before a mutation must not satisfy a post-write check.
  // Concurrent writes can finish across more than one fetch: keep waiting for
  // a read begun after their acknowledgements instead of declaring failure.
  const end=performance.now()+6000;
  while(performance.now()<end) {
    const observed=await refresh();
    if(observed && observedMutation===mutation) return observed;
    await pause(30);
  }
  throw new Error("尚未读到最新状态，请检查控制台连接后重试。");
}
async function waitFor(predicate,timeout=6000,message="尚未观测到预期结果。") {
  const end=performance.now()+timeout;
  while(performance.now()<end) {
    const observed=await read();
    if(predicate(observed)) return observed;
    await pause(100);
  }
  throw new Error(message+" 当前步骤未判定通过，可保留现场检查或重新做本实验。");
}
async function waitCart(expected,nodes="ABC",timeout=6000) {
  return waitFor(s=>{
    const selected=[...nodes].map(id=>getNode(s,id));
    return selected.every(n=>n?.online && n.ready &&
      n.cart.items.length===Object.keys(expected).length &&
      n.cart.items.every(i=>expected[i.sku]===i.quantity)) &&
      new Set(selected.map(n=>n.cart.state_hash)).size===1;
  },timeout,`${nodes.split("").join("/")} 的商品或完整状态尚未达到本步要求。`);
}

function nodeFor(client) { return getNode(state,$(client+"-node").value); }
function summary(node) {
  if(!node?.online) return "服务不可用";
  if(!node.ready) return "数据导入中，尚不可用";
  return node.cart.items.map(i=>i.name+" ×"+i.quantity).join("，") || "购物车为空";
}
function renderClient(client) {
  const node=nodeFor(client), usable=!!(node?.online && node.ready);
  const disabled=busy || inFlight[client] || !!pending[client];
  $(client+"-node").disabled=busy || inFlight[client] || !!pending[client];
  html(client+"-products",Object.entries(state.catalog).map(([sku,p])=>
    `<button class="product" data-client="${client}" data-add="${sku}" aria-label="${client==="phone"?"手机端":"电脑端"}添加${p.name}" ${!usable||disabled?"disabled":""}><span class="emoji">${p.emoji}</span><span class="product-name">${p.name} ＋</span><span class="price">${money(p.price)}</span></button>`).join(""));
  if(!usable) {
    html(client+"-items",`<p class="offline-message">${node?.online?"节点正在导入数据，尚未开放服务。":"节点不可访问。另一端仍可继续操作；重启本节点后再查看。"}</p>`);
    $(client+"-quantity").textContent="不可读取"; $(client+"-total").textContent="—";
  } else {
    $(client+"-quantity").textContent=node.cart.quantity+" 件";
    $(client+"-total").textContent=money(node.cart.total_cents);
    html(client+"-items",node.cart.items.length?node.cart.items.map(item=>
      `<div class="cart-row"><span class="item-icon">${item.emoji}</span><div class="item-name">${item.name}<small>${money(item.price)} / 件</small></div><div class="quantity-control"><button data-client="${client}" data-minus="${item.sku}" aria-label="${client==="phone"?"手机端":"电脑端"}减少一件${item.name}" ${disabled?"disabled":""}>−</button><b>${item.quantity}</b><button data-client="${client}" data-add="${item.sku}" aria-label="增加一件${item.name}" ${disabled?"disabled":""}>＋</button></div><button class="remove-button" data-client="${client}" data-remove="${item.sku}" ${disabled?"disabled":""}>移除</button></div>`).join(""):'<p class="empty">购物车是空的<br>选择上方商品，添加第一件。</p>');
  }
  $(client+"-feedback").className="client-feedback"+(pending[client]?" warning":"");
  html(client+"-feedback",escapeHtml(feedback[client])+(pending[client]?`<br><button class="retry" data-retry="${client}" ${busy||inFlight[client]?"disabled":""}>使用同一请求 ID 重试 ↗</button>`:""));
}
function renderComparison() {
  const a=nodeFor("phone"), b=nodeFor("desktop");
  const available=a?.online && a.ready && b?.online && b.ready;
  const equal=available && JSON.stringify(a.cart.items.map(i=>[i.sku,i.quantity]))===JSON.stringify(b.cart.items.map(i=>[i.sku,i.quantity]));
  $("comparison").className="comparison "+(equal?"same":"different");
  html("comparison",`<strong>${!available?"有一端暂不可用":equal?"两端商品相同":"两端商品暂时不同"}</strong><span>手机（${a?.node||"—"}）：${escapeHtml(summary(a))}</span><span>电脑（${b?.node||"—"}）：${escapeHtml(summary(b))}</span>`);
}
function vectorText(vv) {
  return Object.entries(vv).sort().map(([a,n])=>`${a.split("@")[0]}·${(a.split("@")[1]||"").slice(0,4)}: ${n}`).join(", ")||"∅";
}
function renderReplicas() {
  const expanded=new Set([...document.querySelectorAll(".raw-details[open]")].map(d=>d.dataset.node));
  const scrolls=new Map([...document.querySelectorAll(".raw-details[open]")].map(d=>[d.dataset.node,d.querySelector("pre")?.scrollTop||0]));
  html("replicas",state.nodes.map(n=>{
    const cart=n.cart, dormant=n.node==="D"&&!n.online&&!state.migration;
    return `<article class="card replica ${dormant?"dormant":""}"><div class="node-heading"><span class="node-name"><i class="node-led ${!n.online?"offline":!n.ready?"joining":""}"></i>节点 ${n.node}</span><span class="pill ${n.online&&n.ready?"green":""}">${!n.online?"离线":n.ready?"服务中":"导入中"}</span></div><div class="node-content">${escapeHtml(summary(n))}</div><div class="vector">${cart?escapeHtml(vectorText(cart.vv)):"不可观测"}</div><div class="node-meta">${cart?`添加标记 ${cart.add_count} · 删除记录 ${cart.tombstone_count}`:"磁盘数据保留"}${n.clock_offset?` · 时钟 ${n.clock_offset}s`:""}</div>${cart?.request_conflicts?.length?'<p class="hint">检测到请求 ID 复用冲突</p>':""}<div class="node-footer"><span>${n.online?"进程 "+n.pid:"独立数据库"}</span><button class="text-button" data-node="${n.node}" data-node-action="${n.online?"stop":"start"}" ${busy?"disabled":""}>${n.online?"停止进程":"启动节点"}</button></div>${cart?`<details class="raw-details" data-node="${n.node}" ${expanded.has(n.node)?"open":""}><summary>元数据与节点日志</summary><pre>${escapeHtml(JSON.stringify({actor:n.actor,state:cart.raw,events:n.events},null,2))}</pre></details>`:""}</article>`;
  }).join(""));
  document.querySelectorAll(".raw-details[open]").forEach(d=>{d.querySelector("pre").scrollTop=scrolls.get(d.dataset.node)||0;});
  html("relations",state.relations.map(r=>`<span class="${r.relation}">${r.left} ↔ ${r.right} · ${relationshipNames[r.relation]}</span>`).join(""));
}
function renderNetwork() {
  const net=state.network, faults=[];
  if(net.blocked.length) faults.push("断开的方向："+net.blocked.map(([a,b])=>a+"→"+b).join("、"));
  if(net.manual) faults.push("自动投递已暂停");
  if(net.repair_enabled===false) faults.push("周期补同步已暂停");
  if(net.rules?.length) faults.push(net.rules.length+" 条单向传输规则");
  if(net.delay_ms) faults.push("消息延迟 "+net.delay_ms/1000+" 秒");
  if(net.copies>1) faults.push("同一消息投递 "+net.copies+" 份");
  if(net.reorder) faults.push("乱序开启");
  $("network-badge").textContent=faults.length?"已注入故障":"正常";
  $("network-badge").className="pill "+(faults.length?"amber":"green");
  $("communication").textContent=net.blocked.length?"已分区":faults.length?"有故障":"正常";
  $("fault-summary").textContent=faults.join("；")||"节点之间正常通信。写入后立即触发同步。";
  const positions={A:[26,44],B:[100,18],C:[100,76]};
  if(getNode(state,"D")?.online) positions.D=[177,47];
  const pairs=Object.keys(positions).flatMap((a,i)=>Object.keys(positions).slice(i+1).map(b=>[a,b]));
  html("topology",`<svg viewBox="0 0 200 96" role="img" aria-label="节点实际通信关系">${pairs.map(([a,b])=>{
    const off=!getNode(state,a).online||!getNode(state,b).online, blocked=net.blocked.some(([s,t])=>(s===a&&t===b)||(s===b&&t===a));
    return `<line x1="${positions[a][0]}" y1="${positions[a][1]}" x2="${positions[b][0]}" y2="${positions[b][1]}" stroke="${off?"#b2b8b7":blocked?"#c8854c":"#65a28d"}" stroke-width="2" ${off||blocked?'stroke-dasharray="5 5"':""}/>`;
  }).join("")}${Object.entries(positions).map(([n,[x,y]])=>`<circle cx="${x}" cy="${y}" r="15" fill="${getNode(state,n).online?"#e1f3e8":"#e5e7e5"}"/><text x="${x}" y="${y+4}" fill="#38564d" text-anchor="middle" font-size="12" font-weight="bold">${n}</text>`).join("")}</svg>`);
  $("queue-count").textContent=net.queued; $("delivered-count").textContent=net.counters.delivered; $("dropped-count").textContent=net.counters.dropped;
  const m=state.migration;
  html("migration-status",m?`<b>${escapeHtml(m.target||"D")}：${{running:"正在导入",paused:"已暂停",interrupted:"导入中断，尚不可用",complete:"导入完成，已开放服务"}[m.status]||m.status}</b> · ${m.sent}/${m.total}<div class="progress-track"><span style="width:${100*m.sent/m.total}%"></span></div>`:"");
}
function renderMessages() {
  if(!state) return;
  const data=messagesData?.cart_id===state.cart_id?messagesData:state, operations=data.operations||[];
  const key=o=>o.request_id+":"+o.fingerprint;
  const title=o=>`${o.node} · ${o.operation.kind==="add"?"添加 1 件":"移除已看到的"}${state.catalog[o.operation.sku]?.name||o.operation.sku}`;
  if(messageSelection!=="follow"&&!operations.some(o=>key(o)===messageSelection)) messageSelection="follow";
  html("message-choice",'<option value="follow">自动跟踪最新操作</option>'+operations.map((o,i)=>
    `<option value="${escapeHtml(key(o))}">${i+1}. ${escapeHtml(title(o))}</option>`).join(""));
  $("message-choice").value=messageSelection;
  const operation=messageSelection==="follow"?operations.at(-1):operations.find(o=>key(o)===messageSelection);
  $("background-summary").textContent=`后台约每 ${data.repair_interval||10} 秒补同步；无变化的消息已折叠。${state.network.blocked.length?"当前存在分区，后台消息也受阻。":""}`;
  if(!operation) {
    $("message-summary").textContent="没有新操作时这里保持安静。";
    html("messages",'<p class="hint">添加一件商品后，查看本地保存、节点间传输和重试的实际记录。</p>');
    return;
  }
  const pendingCount=operation.traces.filter(t=>t.status==="queued").length;
  $("message-summary").textContent=`${title(operation)}。收到过此操作记录的节点：${operation.stored_on.join(" / ")||"尚未确认"}。${pendingCount?`仍有 ${pendingCount} 份消息在途。`:"关键记录已保留，后台消息不会将它挤掉。"}`;
  const attempts=operation.attempts.map((a,i)=>{
    const label={sending:"请求正在发送",acknowledged:a.duplicate?"确认成功 · 相同请求已去重":"确认成功 · 本地已保存",reply_lost:"节点已保存，但返回客户端的确认丢失",unknown:"结果未知，尚未确认是否执行",rejected:"节点拒绝请求，未确认写入"}[a.status];
    return `<div class="request-evidence ${a.status==="reply_lost"?"warning":""}"><b>客户端 → ${a.node} · 第 ${i+1} 次请求</b><span>${label}</span></div>`;
  }).join("");
  const groups=new Map();
  for(const t of operation.traces) {
    const k=`${t.source}:${t.target}:${t.group}`;
    if(!groups.has(k)) groups.set(k,[]);
    groups.get(k).push(t);
  }
  const names={write:"本次写入",retry:"重试后同步",merge:"接力同步",recovery:"恢复补齐",repair:"后台补齐"};
  const groupsHTML=[...groups.values()].map(copies=>{
    const first=copies[0],content=first.contents;
    const payload=content?content.items.map(i=>`${state.catalog[i.sku]?.name}×${i.quantity}`).join("、"):"";
    return `<div class="trace-group"><b>${first.source} → ${first.target} <small>#${first.group} · ${names[first.reason]||"同步"}</small></b>${copies.map(t=>{
      const label={queued:"等待投递",blocked:"分区阻断 · 本份未送达",offline:"目标宕机 · 本份未送达",dropped:"投递中断 · 本份未送达",rejected:"目标未开放服务 · 已拒收",delivered:t.changed?"已送达 · 合并了新状态":"已送达 · 内容未变"}[t.status];
      return `<div class="message-row"><span>第 ${t.copy}/${t.copies} 份</span><span ${t.status==="queued"?`data-due="${t.due_at}"`:""}>${label}${t.out_of_order?" · 越序到达":""}</span></div>`;
    }).join("")}${content?`<details class="payload-details"><summary>这份消息带了什么</summary><p>${payload||"没有有效商品"}；删除记录 ${content.removed} 条。</p><p>${copies.filter(t=>t.delivered_at).map(t=>`第 ${t.copy} 份送达：${new Date(t.delivered_at*1000).toLocaleTimeString("zh-CN",{hour12:false})}`).join("；")}</p></details>`:""}</div>`;
  }).join("");
  // Stable order: only the selected operation's evidence can change this area.
  const expanded=[...$("messages").querySelectorAll(".payload-details")].map(d=>d.open);
  html("messages",attempts+groupsHTML+(groups.size?"":'<p class="hint">尚未观测到与本次操作有关的同步消息。</p>'));
  $("messages").querySelectorAll(".payload-details").forEach((d,i)=>{d.open=!!expanded[i];});
  updateCountdowns();
}
function updateCountdowns() {
  document.querySelectorAll("#messages [data-due]").forEach(e=>{
    const seconds=Math.max(0,Math.ceil(Number(e.dataset.due)-Date.now()/1000));
    const text=seconds?`在途 · 约 ${seconds}s 后尝试投递`:"等待实际投递";
    if(e.textContent!==text) e.textContent=text;
  });
}
async function readMessageLog() {
  await refreshMessages();
  if(!messagesData) return;
  const b=messagesData.background;
  $("background-stats").textContent=`本轮后台补同步：${b.broadcasts} 次发送，${b.delivered} 份送达，其中 ${b.changed} 份合并了新状态。`;
  $("raw-message-log").textContent=JSON.stringify(messagesData.transmissions,null,2);
}
function renderEvents() {
  const relevant=state.events.filter(e=>(!e.detail.cart_id||e.detail.cart_id===state.cart_id)&&(!e.detail.carts||e.detail.carts.some(c=>c.cart_id===state.cart_id)));
  html("events",relevant.slice(0,25).map(e=>`<div class="event"><time>${new Date(e.timestamp*1000).toLocaleTimeString("zh-CN",{hour12:false})}</time><span class="event-dot"></span><div class="event-text">${escapeHtml(e.message)}${e.detail.request_id?`<small>request ${escapeHtml(e.detail.request_id)}</small>`:""}</div></div>`).join(""));
}
function renderGuide() {
  const done=stepIndex>=scene.steps.length, current=scene.steps[stepIndex];
  for(const advanced of [false,true]) html(advanced?"scene-advanced":"scene-main",CartLabScenes.filter(s=>!!s.advanced===advanced).map((s,i)=>`<button data-scene="${s.id}" class="scene-tab ${s.id===scene.id?"active":""}" aria-pressed="${s.id===scene.id}" ${busy?"disabled":""}><small>${advanced?"进阶":String(i+1).padStart(2,"0")}</small>${s.name}</button>`).join(""));
  $("guide-title").textContent=scene.name; $("guide-lead").textContent=scene.lead;
  $("guide-count").textContent=done?"验证完成":`步骤 ${stepIndex+1} / ${scene.steps.length}`;
  html("guide-steps",scene.steps.map((s,i)=>`<li class="${i<stepIndex?"done":i===stepIndex?"current":""}"><span>${i<stepIndex?"✓":i+1}</span>${s.label}</li>`).join(""));
  $("guide-action").textContent=done?"本次实验结论":current.action;
  $("guide-description").textContent=done?scene.conclusion:current.detail;
  $("guide-expected").textContent=done?"可切换下一个场景，系统会自动解除故障、准备独立购物车。":current.expected;
  $("guide-next").textContent=busy?"正在执行与核对…":done?"重新做本实验 ↻":sceneError?"重新准备本实验 ↻":current.action+" →";
  $("guide-next").disabled=busy; $("new-experiment").disabled=busy;
  $("guide-result").className="observed"+(sceneError?" failed":evidence.length?" passed":"");
  $("guide-result").textContent=sceneError?"未通过："+sceneError:busy?"正在读取实际节点状态，结果验证通过后才进入下一步。":evidence.length?"已验证 · "+evidence.at(-1).result:"实际结果将在执行后显示；上方“应该看到”只是预期。";
  html("guide-history",evidence.map(e=>`<li><b>${escapeHtml(e.action)}</b>：${escapeHtml(e.result)}</li>`).join(""));
  $("guide-concept").textContent=scene.concept;
}
function render() {
  if(!state) return;
  $("cart-id").textContent="当前购物车："+state.cart_id;
  $("online-count").textContent=state.nodes.filter(n=>n.online&&n.ready).length;
  $("convergence").textContent=state.compared_nodes.length<2?"待观测":state.converged?"状态一致":"尚未一致";
  $("convergence").style.color=state.converged?"#b0e4c1":"#efcd99";
  $("convergence-caption").textContent="本次读取 "+(state.compared_nodes.join(" / ")||"无可服务节点");
  renderGuide(); renderClient("phone"); renderClient("desktop"); renderComparison();
  renderReplicas(); renderNetwork(); renderMessages(); renderEvents();
}

async function sendOperation(payload) {
  const clients=["phone","desktop"].filter(c=>$(c+"-node").value===payload.node);
  clients.forEach(c=>{inFlight[c]=true; feedback[c]="正在请求节点 "+payload.node+"…";});
  render();
  try {
    const result=await api("/api/operation",payload);
    // Use only the responding node's committed state. Peers update on real reads.
    if(state?.cart_id===payload.cart_id) {
      const nodes=state.nodes.map(n=>n.node===payload.node?{...n,cart:result.cart}:n);
      const serving=nodes.filter(n=>n.online&&n.ready);
      // Keep past observations immutable (e.g. clock/counter before-and-after).
      state={...state,nodes,relations:[],converged:serving.length>=2&&
        new Set(serving.map(n=>n.cart.state_hash)).size===1};
    }
    clients.forEach(c=>{pending[c]=null; feedback[c]=result.duplicate?"相同请求已去重 · 没有重复添加":"写入成功 · "+payload.node+" 已本地持久化 · 确认 1 份";});
    return result;
  } catch(error) {
    clients.forEach(c=>{pending[c]=(!error.status||error.payload?.ambiguous)?payload:null; feedback[c]=error.message;});
    throw error;
  } finally {
    clients.forEach(c=>inFlight[c]=false); render(); scheduleRefresh();
  }
}
async function op(c,label,node,kind,sku,observed=[]) {
  c.requests ||= {};
  const payload=c.requests[label] ||= {node,cart_id:c.cartId,request_id:uid(),operation:{kind,sku,observed}};
  return sendOperation(payload);
}
async function remove(c,label,node,sku) {
  if(c.requests?.[label]) return sendOperation(c.requests[label]);
  const s=await read(), tags=getNode(s,node)?.cart?.items.find(i=>i.sku===sku)?.tags||[];
  return op(c,label,node,"remove",sku,tags);
}
async function prepare(c,apple=false,seed=true) {
  epoch++; observedMutation=-1;
  messagesData=null;messageSelection="follow";
  const result=await api("/api/experiment/new",{seed});
  c.cartId=result.cart_id; c.runId=uid(); c.requests={};
  pending.phone=pending.desktop=null;
  $("phone-node").value="A"; $("desktop-node").value="B";
  feedback.phone=feedback.desktop="准备初始商品，正在核对三个副本。";
  await waitCart(seed?{milk:1}:{});
  if(apple) {await op(c,"initial-apple","A","add","apple"); await waitCart({milk:1,apple:1});}
  feedback.phone=feedback.desktop="初始状态已核对 · A/B/C 一致";
}
function showClaim(client,node,message) {
  $(client+"-node").value=node;
  feedback[client]=message;
  render();
}
const runner={api,read,waitFor,waitCart,op,remove,prepare,getNode,qty,assert,showClaim,
  network:body=>api("/api/network",body),node:(node,action)=>api("/api/node",{node,action})};
async function guidedNext() {
  if(busy) return;
  if(sceneError || stepIndex>=scene.steps.length) return selectScene(scene.id);
  busy=true; sceneError=""; renderGuide(); render();
  const current=scene.steps[stepIndex];
  try {
    // Restore the displayed inputs so each explanation matches the actual node.
    $("phone-node").value="A"; $("desktop-node").value="B";
    const result=await current.run(runner,context);
    evidence.push({action:current.action,result,verified_at:new Date().toISOString()});
    stepIndex++;
  } catch(error) {sceneError=error.message;}
  finally {busy=false; render(); await refresh();}
}
async function selectScene(id) {
  if(busy) return;
  scene=CartLabScenes.find(s=>s.id===id); stepIndex=0; evidence=[]; context={}; sceneError="";
  document.querySelector(".scene-more").open=!!scene.advanced;
  renderGuide(); await guidedNext();
}
async function manualOperation(client,kind,sku,minus=false,retry=false) {
  if(busy || inFlight[client]) return;
  let payload=pending[client];
  if(!retry) {
    const n=nodeFor(client); if(!n?.online||!n.ready||payload) return;
    const tags=n.cart.items.find(i=>i.sku===sku)?.tags||[];
    payload={node:n.node,cart_id:state.cart_id,request_id:uid(),operation:{kind,sku,observed:kind==="remove"?(minus?tags.slice(0,1):tags):[]}};
  }
  if(payload) await sendOperation(payload);
}
async function action(fn) {
  try {await fn(); await refresh();}
  catch(error) {toast(error.message,true);}
}
document.addEventListener("click",event=>{
  const b=event.target.closest("button"); if(!b||b.disabled) return;
  if(b.dataset.tab) {
    showTab(b.dataset.tab);
  } else if(b.dataset.scene) action(()=>selectScene(b.dataset.scene));
  else if(b.dataset.add) action(()=>manualOperation(b.dataset.client,"add",b.dataset.add));
  else if(b.dataset.minus) action(()=>manualOperation(b.dataset.client,"remove",b.dataset.minus,true));
  else if(b.dataset.remove) action(()=>manualOperation(b.dataset.client,"remove",b.dataset.remove));
  else if(b.dataset.retry) action(()=>manualOperation(b.dataset.retry,null,null,false,true));
  else if(b.dataset.nodeAction&&!busy) action(()=>api("/api/node",{node:b.dataset.node,action:b.dataset.nodeAction}));
});
function showTab(id) {
  document.querySelectorAll(".tab-panel").forEach(p=>p.classList.toggle("hidden",p.id!==id));
  document.querySelectorAll(".nav-item").forEach(n=>n.classList.toggle("active",n.dataset.tab===id));
  if(id==="lessons") renderLesson();
  window.scrollTo(0,0);
}
function renderLesson() {
  const lesson=CartLabLessons.lessons.find(l=>l.id===lessonId)||CartLabLessons.lessons[0];
  html("lesson-choice",CartLabLessons.lessons.map(l=>`<option value="${l.id}">${escapeHtml(l.title)}</option>`).join(""));
  $("lesson-choice").value=lesson.id;
  const list=items=>"<ul>"+items.map(t=>`<li>${escapeHtml(t)}</li>`).join("")+"</ul>";
  html("lesson-content",`<article class="card lesson-article"><h2>${escapeHtml(lesson.title)}</h2><p class="lesson-question">${escapeHtml(lesson.question)}</p><h3>这次要验证什么</h3><p>${escapeHtml(lesson.goal)}</p><h3>开始前是什么样</h3><p>${escapeHtml(lesson.setup)}</p><h3>一步一步看</h3><ol class="lesson-steps">${lesson.steps.map((s,i)=>`<li><h4>第 ${i+1} 步 · ${escapeHtml(s.do)}</h4><p><b>应该看到：</b>${escapeHtml(s.see)}</p><p><b>为什么：</b>${escapeHtml(s.why)}</p></li>`).join("")}</ol><h3>用到了什么原理</h3>${list(lesson.principles)}<h3>怎样才算通过（预期判据）</h3>${list(lesson.success)}<h3>这次没有证明什么</h3>${list(lesson.limits)}<h3>展示时可以这样讲</h3><blockquote>${escapeHtml(lesson.say)}</blockquote>${lesson.faq.map(f=>`<details class="lesson-faq"><summary>${escapeHtml(f.q)}</summary><p>${escapeHtml(f.a)}</p></details>`).join("")}</article>`);
  $("lesson-start").disabled=lesson.id==="hotspot";
  $("lesson-start").textContent=lesson.id==="hotspot"?"热点实验请按上方命令运行":lesson.id===scene.id?"返回当前实验，继续操作 →":"准备并开始这个实验 →";
}
$("explain-scene").onclick=()=>{lessonId=scene.id;showTab("lessons");};
$("lesson-choice").onchange=e=>{lessonId=e.target.value;renderLesson();};
$("lesson-start").onclick=()=>action(async()=>{showTab("workbench");if(lessonId!==scene.id) await selectScene(lessonId);});
$("message-choice").onchange=e=>{messageSelection=e.target.value;renderMessages();};
$("raw-message-details").ontoggle=()=>{if($("raw-message-details").open) readMessageLog();};
$("refresh-message-log").onclick=()=>action(readMessageLog);
$("guide-next").onclick=()=>action(guidedNext);
$("new-experiment").onclick=()=>action(()=>selectScene(scene.id));
for(const client of ["phone","desktop"]) $(client+"-node").onchange=()=>{if(state){renderClient(client);renderComparison();}};
$("export").onclick=()=>{
  if(!state) return toast("尚无可导出的观测。",true);
  const record={exported_at:new Date().toISOString(),experiment:scene.name,verified_steps:evidence,
    current_step:stepIndex,error:sceneError,note:"各节点的非原子观测；完整状态比较范围见 compared_nodes",...state,
    message_observation:messagesData};
  const url=URL.createObjectURL(new Blob([JSON.stringify(record,null,2)],{type:"application/json"}));
  const link=document.createElement("a");link.href=url;link.download=state.cart_id+"-observation.json";link.click();
  setTimeout(()=>URL.revokeObjectURL(url),1000);
};
html("checklist-body",CartLabScenes.map(s=>`<tr><td>${s.name}</td><td>${s.steps.map(t=>t.label).join(" → ")}</td><td>${s.conclusion}</td></tr>`).join("")+
  "<tr><td>热点请求 50%</td><td>运行 tools/benchmark.py</td><td>核对预期与实际数量，记录吞吐、P95、错误率。</td></tr>");
renderGuide(); refresh();
const eventStream=new EventSource("/api/events");
eventStream.onopen=()=>{streamLive=true;scheduleRefresh();};
eventStream.addEventListener("change",scheduleRefresh);
eventStream.addEventListener("messages",scheduleMessages);
eventStream.onerror=()=>{streamLive=false;$("connection").textContent="通知重连中 · 定时读取";};
setInterval(()=>{if(!streamLive) refresh();},5000);
setInterval(refresh,30000); // Detect failures outside the experiment controller.
setInterval(refreshMessages,10000); // Lightweight statistics; no node queries.
setInterval(updateCountdowns,1000); // Touch only actual in-flight countdowns.
document.addEventListener("visibilitychange",()=>{if(!document.hidden) scheduleRefresh();});
window.addEventListener("pagehide",()=>eventStream.close());

"use strict";

const $ = id => document.getElementById(id);
const ids = ["A", "B", "C", "D"];
const clients = ["phone", "desktop"];
const lanes = ["one", "two"];
const clientNames = {phone:"手机", desktop:"桌面"};
const money = cents => "¥" + (cents/100).toFixed(2);
const reasonNames = {write:"本次写入",retry:"原请求重试",merge:"接力",recovery:"恢复补齐",repair:"后台补同步",manual:"手工快照",duplicate:"复制快照"};
const statusNames = {delivered:"已送达",blocked:"链路阻断",offline:"目标离线",rejected:"目标拒收",dropped:"已丢弃",queued:"排队中"};
const escaped = value => String(value ?? "").replace(/[&<>"']/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
const value = id => $(id).value;
const setText = (id, text) => { if ($(id).textContent !== text) $(id).textContent = text; };
const htmlCache = new Map();
function setHTML(id, markup) {
  if (htmlCache.get(id) === markup) return;
  $(id).innerHTML = markup;
  htmlCache.set(id, markup);
}

const hhmmss = seconds => new Date(seconds * 1000).toLocaleTimeString("zh-CN",{hour12:false});
const nodeOptions = (chosen, allowOff=false) => (allowOff ? '<option value="">断开</option>' : "") + ids.map(n =>
  `<option value="${n}" ${n===chosen?"selected":""}>${n}</option>`).join("");
const num = id => {
  const field = $(id);
  if (!field.checkValidity() || !Number.isFinite(field.valueAsNumber)) {
    field.reportValidity();
    throw new Error("请检查数值范围");
  }
  return field.valueAsNumber;
};

let state, dispatch, active="nodes", selected="A", page=0;
let resets=false, noticeTimer, refreshTimer, reading=false, readAgain=false, packetRead=0;
let actionLog=[], captures=[], loads=[], loadJob=null;
let requests={}, messages={}, clientViews={};
let controllerOnline=false;
const clientBusy = new Set();
const activeButtons = new WeakSet();

let pendingControl;
function api(path, body) {
  const control = body !== undefined && !["/api/operation","/api/cas"].includes(path);
  const previous = pendingControl;
  const dispatch = async () => {
    if (previous && (body !== undefined || path.startsWith("/api/cas?"))) await previous;
    return httpRequest(path, body);
  };
  const response = dispatch();
  if (control) {
    pendingControl = response;
    setText("control-status","控制指令执行中…");
    const finished = () => {
      if (pendingControl === response) {
        pendingControl = null;
        setText("control-status","");
      }
    };
    response.then(finished, finished);
  }
  return response;
}
async function httpRequest(path, body) {
  const options = body===undefined ? {cache:"no-store"} : {method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(body)};
  const response = await fetch(path, options);
  const data = await response.json();
  if (!response.ok) {
    const error = new Error(data.error || "请求失败");
    error.code=response.status;
    error.data=data;
    throw error;
  }
  return data;
}
function report(message, failed=false) {
  actionLog.unshift({at:Date.now()/1000, message, failed});
  actionLog.length=Math.min(actionLog.length,100);
  $("notice").textContent=message;
  $("notice").className=failed?"error":"";
  clearTimeout(noticeTimer);
  noticeTimer=setTimeout(()=>setText("notice",""), 3800);
  if(active==="history") renderHistory();
}
function explain(error) {
  return (error.code ? `HTTP ${error.code} · ` : "") + error.message;
}
function node(id) { return state?.nodes.find(n=>n.node===id); }
function blocked(source,target) { return state.network.blocked.some(([a,b])=>a===source && b===target); }
function items(n) {
  if (!n?.online) return "不可读取";
  if (!n.ready) return "导入中";
  return n.cart.items.map(i=>`${i.name} ×${i.quantity}`).join("、") || "空购物车";
}
function availability(n) { return !n?.online ? "已停止" : n.ready ? "可服务" : "导入中"; }
function vector(n) {
  return Object.entries(n.cart.vv).map(([actor,count])=>actor.split("@")[0]+":"+count).join(" ") || "∅";
}
function currentClientNode(c) { return node(value(c+"-node")); }

const recipes = [
  ["正常双向同步", "手机连 A，电脑连 B。手机加一件苹果，等电脑看到后由电脑移除此项。", "比较两个客户端的商品与 A/B/C 实读，苹果应先出现、再消失。"],
  ["断网后分别修改", "隔离 A。手机经 A 加苹果，电脑经 B 加面包。在观测记录保存对照，再接通 A。", "分区时两端各自写入成功。恢复后两种商品都保留，可保存第二次对照。"],
  ["同时加与删", "先让两端都看到同一件苹果。隔离 A，手机移除苹果，电脑再加一件。然后接通 A。", "A 为 0 件、B 为 2 件。恢复后都为 1 件。展开商品标记可看到仅旧苹果被删除。"],
  ["节点宕机与恢复", "先加一件商品并等各副本收到。停止 A，电脑通过 B 写入另一件商品，再启动 A。", "A 离线期间显示不可读取，B 可写。A 重启后从原库恢复并补齐 B 的新增。"],
  ["复制或丢弃一份消息", "暂停自动投递，手机加苹果。在消息队列筛选 A → B，复制一份，再任选一份丢弃，手动投递另一份。", "丢弃的是选中的包。投递成功后 B 得到一件苹果。也可以把原件和副本都送达，检查第二次合并的结果。"],
  ["写入确认丢失与重试", "选 A，点击丢弃下次写入确认，再通过手机加苹果。保留选中的请求，点击重发原请求。也可以先把手机改连 B 再重发。", "核对第一次的 HTTP 504、相同的请求 ID 和苹果数量。跨分区重发时可能先各自确认，恢复传播后应只有一件。"],
  ["延迟与乱序", "暂停自动投递，手机在 A 加一件苹果，再移除。到消息队列筛选 A → B，先投递带删除记录的新包，后投递含苹果的旧包。", "旧包显示越序到达，B 的删除记录仍在，苹果保持为 0。要试自然延迟，给 A → B 设置 10 秒延迟，再恢复自动投递。"],
  ["时钟回拨", "先在 A 写一次并记下 A 的版本。把 A 的时钟偏移设为 −60 秒，再写一次，观察后点击恢复时钟。", "节点显示时间回退，版本中 A 的计数继续增加。"],
  ["CAS 同节点与跨节点", "两个 CAS 请求选同一键、同一节点、同一期望版本，一起发送。换一个新键，改为分别向 A、B 发送。", "第一次应出现 200 和 409，第二次两个节点都能在自己的版本 0 上成功。"],
  ["迁移中途失败", "准备 A → D 快照，逐片发送。中途停止 D，重启后让电脑连 D 尝试加购。再重新取快照，逐片收齐并提交。", "提交前普通写入被拒绝。提交后 D 能读写并补齐其他副本上的更新。"],
  ["热点与并发请求", "打开热点压测，保持总请求、并发数和节点相同，分别以 0% 与 50% 热点运行。", "对照每轮的热点确认写入数与实读数量，再观察吞吐、P50 和 P95。每轮数据只代表本机当次负载。"]
];
function renderRecipe() {
  const [,steps,check]=recipes[Number(value("recipe-choice"))||0];
  setHTML("recipe-content",`<p><b>操作</b>　${escaped(steps)}</p><p><b>观察</b>　${escaped(check)}</p>`);
}
function deviceWindow(c,index) {
  const name=clientNames[c]+"端";
  const chrome=c==="phone" ? '<span class="earpiece"></span>' : '<i></i><i></i><i></i>';
  return `<article class="device-window ${c}" id="${c}-window" aria-label="${name}购物窗口">
    <div class="device-chrome" aria-hidden="true">${chrome}</div>
    <div class="device-toolbar"><div><h3>${name}</h3><span id="${c}-status" class="device-status">等待读取</span></div>
      <label class="route">节点<select id="${c}-node" aria-label="${name}连接节点">${nodeOptions(ids[index],true)}</select></label></div>
    <div class="shop-body"><div class="basket-heading"><h4>我的购物车</h4><span class="unit-count" id="${c}-quantity">—</span></div>
      <div id="${c}-items" class="basket-items"></div>
      <div class="basket-total"><span>商品合计</span><strong id="${c}-total">—</strong></div>
      <div class="last-change" id="${c}-change" role="status"></div>
      <div class="catalog-heading"><h4>选购商品</h4><span>点商品加一件</span></div>
      <div id="${c}-products" class="catalog"></div>
    </div>
    <div class="receipt"><output id="${c}-feedback">本端尚未发送请求</output>
      <button id="${c}-retry" data-client="${c}" data-op="retry" hidden>重发原请求</button></div>
  </article>`;
}

function productButton(c,sku,product) {
  const name=escaped(product.name), key=escaped(sku);
  return `<button class="shop-product sku-${key}" data-client="${c}" data-op="add" data-sku="${key}" aria-label="${clientNames[c]}端选购${name}">
    <span class="product-pic" aria-hidden="true">${escaped(product.emoji)}</span><span class="product-name">${name}</span>
    <span class="product-price">${money(product.price)}</span><span class="add-mark" aria-hidden="true">+</span></button>`;
}

function cartRow(c,item) {
  const {sku,quantity,price}=item;
  const name=escaped(item.name), key=escaped(sku), device=clientNames[c]+"端";
  return `<div class="basket-row sku-${key}" data-item="${key}">
    <span class="row-picture" aria-hidden="true">${escaped(item.emoji)}</span>
    <span class="row-name">${name}<small>${money(price)} / 件</small></span>
    <div class="stepper"><button data-client="${c}" data-op="minus" data-sku="${key}" aria-label="${device}减少一件${name}">−</button><b aria-label="${quantity} 件">${quantity}</b><button data-client="${c}" data-op="add" data-sku="${key}" aria-label="${device}增加一件${name}">+</button></div>
    <span class="line-total">${money(price*quantity)}</span><button class="remove-item" data-client="${c}" data-op="remove" data-sku="${key}" aria-label="${device}移除${name}">移除</button>
  </div>`;
}

function counts(cart) {
  return Object.fromEntries(cart.items.map(item=>[item.sku,item.quantity]));
}

function differences(before,after) {
  return Object.entries(state.catalog).filter(([sku])=>(before[sku]||0)!==(after[sku]||0))
    .map(([sku,product])=>({name:product.name,from:before[sku]||0,to:after[sku]||0}));
}

function observeCart(c,replica,cart) {
  if(!cart) {
    delete clientViews[c];
    setText(c+"-change","等待读取当前节点的购物车");
    return;
  }
  const next=counts(cart);
  const previous=clientViews[c];
  if(previous?.node!==replica) {
    setText(c+"-change",`已从节点 ${replica} 读取购物车`);
  } else {
    const changes=differences(previous.counts,next);
    if(changes.length) {
      const update=changes.map(d=>`${d.name} ${d.from} → ${d.to} 件`).join("、");
      setText(c+"-change",`${hhmmss(Date.now()/1000)}　${update}`);
      if(!window.matchMedia("(prefers-reduced-motion: reduce)").matches) {
        $(c+"-total").animate([{backgroundColor:"#f9d593"},{backgroundColor:"transparent"}],{duration:1000});
      }
    }
  }
  clientViews[c]={node:replica,counts:next};
}

function deviceAvailability(replica,cart) {
  if(!controllerOnline) return {tone:"down",label:"读取中断",detail:"控制器暂不可达，等待重新读取。"};
  if(!replica) return {tone:"down",label:"客户端已断开",detail:"在本窗口选择节点，即可读取购物车。"};
  if(!replica.online) return {tone:"down",label:`节点 ${replica.node} 不可用`,detail:"暂时读不到商品。可以切换节点，或点选商品试发请求。"};
  if(!cart) return {tone:"waiting",label:"节点正在导入",detail:"提交前暂不开放购物车，可以点选商品验证写入结果。"};
  const peers=state.nodes.filter(n=>n.online && n.ready && n.node!==replica.node);
  const isolated=peers.length && peers.every(n=>blocked(replica.node,n.node)&&blocked(n.node,replica.node));
  return isolated ? {tone:"waiting",label:"副本隔离 · 可购物"} : {tone:"ok",label:"可购物"};
}

function renderClient(c) {
  const replica=currentClientNode(c);
  const cart=controllerOnline && replica?.online && replica.ready ? replica.cart : null;
  const status=deviceAvailability(replica,cart);
  $(c+"-status").dataset.tone=status.tone;
  setText(c+"-status",status.label);
  const quantity=cart ? `${cart.quantity} 件` : "—";
  setText(c+"-mini",quantity);
  setText(c+"-quantity",quantity);
  setText(c+"-total",cart ? money(cart.total_cents) : "—");
  let rows;
  if(!cart) {
    rows=`<div class="basket-placeholder halted"><b>${escaped(status.label)}</b><small>${escaped(status.detail)}</small></div>`;
  } else if(!cart.items.length) {
    rows='<div class="basket-placeholder"><span class="basket-symbol" aria-hidden="true"></span><b>购物车还是空的</b><small>从下方选一件商品</small></div>';
  } else {
    rows=cart.items.map(item=>cartRow(c,item)).join("");
  }
  setHTML(c+"-items",rows);
  observeCart(c,replica?.node,cart);
  setHTML(c+"-products",Object.entries(state.catalog).map(([sku,product])=>productButton(c,sku,product)).join(""));
  const busy=clientBusy.has(c);
  $(c+"-node").disabled=busy;
  const retry=$(c+"-retry");
  retry.hidden=!requests[c];
  for(const button of $(c+"-window").querySelectorAll("button[data-op]")) {
    button.disabled=resets || busy || !replica || !controllerOnline;
  }
  const receipt=messages[c];
  setText(c+"-feedback",busy ? `正在向节点 ${replica?.node} 发送…` : receipt?.text || "本端尚未发送请求");
  $(c+"-feedback").dataset.tone=busy ? "waiting" : receipt?.tone || "muted";
}

function renderShopping() {
  clients.forEach(renderClient);
  const phone=clientViews.phone, desktop=clientViews.desktop;
  let tone="muted", title="暂时无法对照", detail="等两端都连上可读取的节点后再比较。";
  if(phone && desktop) {
    const diff=differences(phone.counts,desktop.counts);
    tone=diff.length ? "waiting" : "ok";
    title=diff.length ? "两端商品不同" : "两端商品相同";
    detail=diff.length ? diff.map(d=>`${d.name}（手机 ${d.from} / 桌面 ${d.to}）`).join("、")
      : (phone.node===desktop.node ? `两端都连接节点 ${phone.node}` : `手机读 ${phone.node}，桌面读 ${desktop.node}`);
  }
  $("comparison").dataset.tone=tone;
  setText("compare-title",title);
  setText("compare-detail",detail);
  setText("mini-comparison",title);
}

function initForms() {
  setHTML("recipe-choice",recipes.map(([title],index)=>`<option value="${index}">${escaped(title)}</option>`).join(""));
  renderRecipe();
  document.querySelectorAll("[data-node-select]").forEach(el=>el.innerHTML=nodeOptions(el.dataset.nodeSelect));
  setHTML("client-list",clients.map(deviceWindow).join(""));
  setHTML("cas-lanes",lanes.map((id,i)=>`<article class="lane"><h3>请求 ${i+1}</h3>
    <label>节点<select id="${id}-node">${nodeOptions("A")}</select></label>
    <label>期望版本<input id="${id}-version" type="number" min="0" step="1" value="0"></label>
    <label>写入值<input id="${id}-value" value="${i?"电脑":"手机"}"></label><div class="actions">
    <button data-cas="${id}" data-method="read">读取版本</button><button data-cas="${id}" data-method="write">提交 CAS</button></div>
    <output class="feedback" id="${id}-result"></output></article>`).join(""));
  const directions=ids.flatMap(s=>ids.filter(t=>s!==t).map(t=>`<option value="${s}${t}">${s} → ${t}</option>`));
  $("queue-filter").innerHTML='<option value="">全部方向</option>'+directions.join("");
}

function clearSession() {
  requests={}; messages={}; clientViews={}; actionLog=[]; captures=[]; loads=[]; page=0; selected="A"; dispatch=null; packetRead++;
  if(loadJob) loadJob.stop=true;
  for(const el of document.querySelectorAll("select")) {
    const defaults=[...el.options].find(o=>o.defaultSelected);
    el.selectedIndex=defaults?defaults.index:0;
  }
  for(const el of document.querySelectorAll("input")) {
    if(el.type==="checkbox") el.checked=el.defaultChecked;
    else el.value=el.defaultValue;
  }
  for(const lane of lanes) setText(lane+"-result","");
  renderRecipe();
  setText("load-status","");
  renderLoads();
}
function schedule() {
  if(!refreshTimer && !document.hidden) refreshTimer=setTimeout(()=>{refreshTimer=null; refresh();},100);
}
async function refresh() {
  if(reading) {readAgain=true; return;}
  reading=true;
  try {
    const next=await api("/api/status");
    if(state && state.cart_id!==next.cart_id) clearSession();
    state=next;
    controllerOnline=true;
    setText("connection","● 已连接");
    render();
    if(active==="packets") await refreshPackets().catch(e=>setText("queue-count",explain(e)));
  } catch(error) {
    controllerOnline=false;
    setText("connection","控制器连接中断");
    if(state) renderShopping();
  } finally {
    reading=false;
    if(readAgain){readAgain=false;schedule();}
  }
}
async function refreshPackets() {
  const tag=++packetRead;
  const next=await api("/api/dispatch");
  if(tag!==packetRead || active!=="packets" || next.cart_id!==state?.cart_id) return;
  dispatch=next;
  renderQueue();
}
function render() {
  const net=state.network;
  setText("cart-key",state.cart_id);
  setText("overview",`${state.compared_nodes.length} 个可服务副本 · ${state.compared_nodes.length<2?"等待对照":state.converged?"完整状态相同":"完整状态有差异"}`);
  setText("dispatch-mode",net.manual?"恢复自动投递":"暂停自动投递");
  $("dispatch-mode").classList.toggle("held",net.manual);
  setText("queue-badge",String(net.queued));
  const focus=node(selected);
  setHTML("node-picker",state.nodes.map(n=>`<button data-pick="${n.node}" aria-pressed="${n.node===selected}"><b class="node-id">${n.node}</b><span class="node-state ${n.online?"":"offline"}">${availability(n)}</span></button>`).join(""));
  setText("node-title","节点 "+selected);
  setText("node-detail",focus.online ? `PID ${focus.pid} · 时钟偏移 ${focus.clock_offset} 秒 · 观测时间 ${hhmmss(focus.wall_time)} · ${items(focus)}` : "进程已停止，重启将读取该节点的数据库。");
  setText("power",focus.online?"停止进程":"启动进程");
  setText("lose-ack",state.drop_reply.includes(selected)?"取消丢失确认":"丢弃下次写入确认");
  $("apply-clock").disabled=$("clear-clock").disabled=!focus.online;
  const rows=ids.map(a=>`<tr><th scope="row">${a}</th>${ids.map(b=>a===b?'<td class="self">—</td>':
    `<td><button data-edge="${a}${b}" aria-label="${a} 到 ${b} ${blocked(a,b)?"已断":"接通"}" class="${blocked(a,b)?"link-cut":node(a).online&&node(b).online?"link-open":"link-offline"}">${blocked(a,b)?"已断":"接通"}</button></td>`).join("")}</tr>`);
  setHTML("matrix",`<table aria-label="节点间链路配置"><thead><tr><th>从 ↓ 到 →</th>${ids.map(id=>"<th>"+id+"</th>").join("")}</tr></thead><tbody>${rows.join("")}</tbody></table>`);
  const rules=net.rules.map(r=>`${r.source} → ${r.target}　${r.delay_ms/1000} 秒 / ${r.copies} 份 / 待丢 ${r.drop_next} 份`);
  setText("rules-view",rules.join("　｜　") || "各方向使用全局规则。");
  setText("mode-label",net.manual?"已暂停 · 可逐包投递":"自动投递中");
  $("repair").checked=net.repair_enabled;
  $("auto-reorder").checked=net.reorder;
  renderShopping();
  setHTML("replica-list",state.nodes.map(n=>`<div class="replica-row"><b>${n.node}</b><span>${escaped(items(n))}</span><small>${n.online&&n.ready?`删除 ${n.cart.tombstone_count} · ${escaped(vector(n))}`:availability(n)}</small></div>`).join(""));
  setText("raw-state",JSON.stringify(Object.fromEntries(state.nodes.map(n=>[n.node,n.ready?n.cart.raw:availability(n)])),null,2));
  renderImport();
  if(active==="history") renderHistory();
}

function renderImport() {
  const m=state.migration;
  const live=m && ["paused","running"].includes(m.status);
  const statuses={paused:"已暂停",running:"传输中",complete:"已提交",interrupted:"已中断"};
  setText("migration-badge",m?statuses[m.status]:"空闲");
  setText("import-label",m?`${m.source} → ${m.target}　${m.sent} / ${m.total} 片`:"等待准备快照");
  $("import-progress").max=m?.total||8; $("import-progress").value=m?.sent||0;
  let hint="准备后可逐片接收，收齐后可单独验证并提交。";
  if(m?.status==="interrupted") hint=`${m.error}。恢复节点和链路后，可以重新取快照。`;
  if(m?.status==="paused") hint=m.sent===m.total?"分片已收齐。点击校验并提交，目标才会开放服务。":"等待操作员发送下一片。";
  if(m?.status==="running") hint=m.manual?"当前分片完成后暂停。":"正在自动传输分片。";
  if(m?.status==="complete") hint="目标已开放服务，可切换购物客户端验证读写。";
  setText("import-details",hint);
  $("begin-import").disabled=!!live;
  $("import-source").disabled=$("import-target").disabled=!!live;
  $("retry-import").disabled=m?.status!=="interrupted";
  for(const b of document.querySelectorAll("[data-import]")){
    const a=b.dataset.import;
    b.disabled=!live || (a==="step" && (m.status!=="paused"||m.sent===m.total)) ||
               (a==="commit" && (m.status!=="paused"||m.sent!==m.total)) ||
               (a==="pause" && m.manual) || (a==="resume" && !m.manual);
  }
  const target=live||m?.status==="interrupted"?m.target:value("import-target");
  const source=live||m?.status==="interrupted"?m.source:value("import-source");
  setText("import-power",`${node(target)?.online?"停止":"启动"}目标 ${target}`);
  setText("import-link",`${blocked(source,target)?"接通":"断开"} ${source} → ${target}`);
}
function switchTab(name) {
  active=name;
  for(const el of document.querySelectorAll("[data-panel]")) el.hidden=el.dataset.panel!==name;
  for(const b of document.querySelectorAll("[data-tab]")){
    if(b.dataset.tab===name) b.setAttribute("aria-current","page");
    else b.removeAttribute("aria-current");
  }
  $("tool-body").scrollTop=0;
  if(state){render();if(name==="packets") refreshPackets().catch(e=>report(explain(e),true));}
}
function payload(t) {
  if(!t.contents) return "其他购物车的快照";
  const items=t.contents.items.map(i=>`${state.catalog[i.sku]?.name||i.sku} ×${i.quantity}`);
  return (items.join("、")||"空购物车")+` · 删除记录 ${t.contents.removed}`;
}
function renderQueue() {
  if(!dispatch) return;
  const filter=value("queue-filter");
  const match=t=>(!filter||filter===t.source+t.target)&&($("show-repair").checked||t.reason!=="repair");
  const packets=dispatch.pending.filter(match);
  page=Math.min(page,Math.max(0,Math.ceil(packets.length/8)-1));
  setText("queue-badge",String(dispatch.network.queued));
  setText("queue-count",`符合筛选 ${packets.length} / 共 ${dispatch.pending.length} 份`);
  setHTML("queue-list",packets.slice(page*8,page*8+8).map(t=>`<article class="packet"><div class="packet-head">
    <b>${t.source} → ${t.target}</b><small class="packet-id">#${t.id} · ${reasonNames[t.reason]||"同步"} · 第 ${t.copy}/${t.copies} 份</small></div>
    <p>${escaped(payload(t))} · ${(dispatch.network.manual?"已扣留 · 到期 ":"计划 ")+hhmmss(t.due_at)}${t.copied_from?" · 复制自 #"+t.copied_from:""}</p>
    <div class="actions">${Object.entries({deliver:"立即投递",duplicate:"复制一份",delay:"改延迟",drop:"丢弃"}).map(([act,label])=>`<button data-packet="${t.id}" data-adjust="${act}" aria-label="${label} #${t.id}">${label}</button>`).join("")}</div></article>`).join("") || '<p class="empty">当前筛选下没有待投递消息。</p>');
  setHTML("queue-pages",packets.length>8?`<button data-page="-1" ${page===0?"disabled":""}>上一页</button><span>${page+1}/${Math.ceil(packets.length/8)}</span><button data-page="1" ${(page+1)*8>=packets.length?"disabled":""}>下一页</button>`:"");
  setHTML("deliveries",dispatch.recent.filter(match).slice(0,24).map(t=>`<div class="log-line"><b>#${t.id}　${t.source} → ${t.target}</b>　${statusNames[t.status]}${t.status==="delivered"?` · ${t.changed?"合并了新状态":"内容未变"} · ${hhmmss(t.delivered_at)}`:""}${t.out_of_order?" · 越序到达":""}<br><small>${escaped(payload(t))}</small></div>`).join("")||'<p class="empty">等待本轮实际投递结果。</p>');
}

function renderHistory() {
  setHTML("captures",captures.map((s,i)=>`<div class="capture"><b>对照 ${captures.length-i} · ${hhmmss(s.at)}</b><div>${s.nodes.map(n=>escaped(n.node+" "+items(n))).join(" ｜ ")}</div>
    <div>${s.network.blocked.length} 条断链 · ${s.network.queued} 份排队</div><details><summary>版本、哈希和链路</summary><pre>${escaped(JSON.stringify({nodes:s.nodes.map(n=>({node:n.node,online:n.online,ready:n.ready,vv:n.cart?.vv,hash:n.cart?.state_hash,removed:n.cart?.raw.removed})),network:s.network},null,2))}</pre></details></div>`).join(""));
  setHTML("action-history",actionLog.map(h=>`<div class="log-line"><time>${hhmmss(h.at)}</time>${escaped(h.message)}</div>`).join("")||'<p class="empty">等待手动操作。</p>');
  setHTML("event-history",state.events.slice(0,35).map(e=>`<div class="log-line"><time>${hhmmss(e.timestamp)}</time>${escaped(e.message)}</div>`).join(""));
}
async function operate(c, action, sku) {
  if(clientBusy.has(c)) return;
  const id=value(c+"-node"), cart=state.cart_id;
  if(!id) throw new Error("请先连接节点");
  let req=requests[c];
  if(action!=="retry"){
    const tags=node(id)?.cart?.items.find(i=>i.sku===sku)?.tags||[];
    const observed=action==="minus"?tags.slice(0,1):action==="remove"?tags:[];
    req={cart_id:cart,request_id:crypto.randomUUID(),operation:{kind:action==="add"?"add":"remove",sku,observed}};
    requests[c]=req;
  }
  if(!req || req.cart_id!==cart) throw new Error("本轮还没有可重发的请求");
  clientBusy.add(c);
  render();
  try {
    const result=await api("/api/operation",{...req,node:id});
    if(state.cart_id!==cart) return;
    const item=state.catalog[req.operation.sku].name;
    const outcome=result.duplicate ? "相同请求已去重" : req.operation.kind==="add" ? "已保存添加" : "已保存移除";
    messages[c]={tone:"ok",text:`${id} · ${outcome}${result.duplicate?"":" · "+item} · ID ${req.request_id.slice(0,8)}`};
    report(clientNames[c]+" → "+messages[c].text);
  } catch(error) {
    if(state.cart_id===cart) {
      const why=error.code===504 && error.data?.ambiguous ? "HTTP 504 · 确认未收到，可重发原请求" : explain(error);
      messages[c]={tone:"down",text:`${id} · ${why} · ID ${req.request_id.slice(0,8)}`};
    }
    throw error;
  } finally {clientBusy.delete(c);render();}
}
function casKey() {
  const name=value("cas-key").trim();
  if(!/^[a-zA-Z0-9_.:@-]{1,60}$/.test(name)) throw new Error("键名可用 1–60 个字母、数字或 _ . : @ -");
  return state.cart_id+"."+name;
}
async function compare(lane, method) {
  const key=casKey(), id=value(lane+"-node");
  let text;
  try {
    const current=method==="read" ?
      await api("/api/cas?"+new URLSearchParams({key,node:id})) :
      await api("/api/cas",{key,node:id,expected:num(lane+"-version"),value:value(lane+"-value")});
    if(method==="read") $(lane+"-version").value=current.version;
    text=`HTTP 200 · ${method==="read"?"读到":"写入成功"}版本 ${current.version} · 值 ${JSON.stringify(current.value)}`;
  } catch(e) {
    text=e.code===409?`HTTP 409 · 版本冲突，当前版本 ${e.data.version} · 值 ${JSON.stringify(e.data.value)}`:explain(e);
  }
  setText(lane+"-result",text);
  report(`CAS · ${id} · ${text}`);
}
async function network(body, description) {
  await api("/api/network",body);
  report(description);
}
async function power(id) {
  const action=node(id)?.online?"stop":"start";
  await api("/api/node",{node:id,action,joining:false});
  report(`节点 ${id} ${action==="stop"?"进程已停止":"进程已启动"}`);
}
function importRoute() {
  const m=state.migration;
  return m && ["paused","running","interrupted"].includes(m.status) ? [m.source,m.target] : [value("import-source"),value("import-target")];
}
function renderLoads() {
  setHTML("load-results",loads.map(r=>`<div class="load-row"><b>${r.node} · 热点 ${r.share}% · 完成 ${r.count} 次</b>
    <div>HTTP 成功 ${r.ok} · 失败或未知 ${r.count-r.ok} · 热点确认写入 ${r.hotAck} · 节点实读 ${r.actual??"不可读取"}</div>
    <div>吞吐 ${r.rate} 请求/秒 · P50 ${r.p50} ms · P95 ${r.p95} ms</div></div>`).join(""));
}
async function runLoad() {
  if(loadJob) return;
  const total=num("load-count"), workers=num("load-workers"), share=num("load-share"), id=value("load-node");
  const hotCount=Math.round(total*share/100), cart=state.cart_id, prefix=cart+"-load-"+crypto.randomUUID().slice(0,8);
  const job={stop:false};
  loadJob=job;
  $("run-load").disabled=true; $("stop-load").disabled=false;
  const times=[];
  let issued=0, ok=0, hotAck=0;
  const start=performance.now();
  async function worker(){
    while(issued<total && !job.stop && state.cart_id===cart){
      const n=issued++;
      const hot=Math.floor((n+1)*hotCount/total)>Math.floor(n*hotCount/total);
      const key=hot?prefix:prefix+"-"+n;
      const t=performance.now();
      try{
        await api("/api/operation",{node:id,cart_id:key,request_id:crypto.randomUUID(),operation:{kind:"add",sku:"apple"}});
        ok++;
        if(hot) hotAck++;
      } catch(_) { /* All completed attempts, including failures, contribute to latency. */ }
      times.push(performance.now()-t);
      if(state.cart_id===cart) setText("load-status",`已完成 ${times.length} / ${total} 次请求`);
    }
  }
  try {
    await Promise.all(Array.from({length:Math.min(workers,total)},worker));
    const duration=performance.now()-start;
    const read=await api("/api/status?cart_id="+encodeURIComponent(prefix));
    const hotNode=read.nodes.find(n=>n.node===id);
    if(state.cart_id!==cart) return;
    times.sort((a,b)=>a-b);
    const percentile=q=>Math.round(times[Math.max(0,Math.ceil(times.length*q)-1)]||0);
    loads.unshift({node:id,share,count:times.length,ok,hotAck,actual:hotNode?.ready?hotNode.cart.quantity:null,
      rate:+(times.length*1000/Math.max(duration,1)).toFixed(1),p50:percentile(.5),p95:percentile(.95)});
    loads.length=Math.min(loads.length,12);
    renderLoads();report("本轮负载已结束，结果已记录");
  } finally {
    loadJob=null; $("run-load").disabled=false; $("stop-load").disabled=true;
  }
}
async function capture() {
  const s=await api("/api/status");
  captures.unshift({at:Date.now()/1000,cart_id:s.cart_id,nodes:s.nodes,network:s.network});
  captures.length=Math.min(captures.length,16);
  report("已保存此刻的节点与链路对照");
}
async function exportRecord(){
  const snapshot=await api("/api/status");
  const packets=await api("/api/dispatch");
  const record={exported_at:new Date().toISOString(),cart_id:snapshot.cart_id,status:snapshot,packets,captures,actions:actionLog,loads};
  const url=URL.createObjectURL(new Blob([JSON.stringify(record,null,2)],{type:"application/json"}));
  const a=document.createElement("a");a.href=url;a.download=snapshot.cart_id+"-自由实验.json";a.click();
  setTimeout(()=>URL.revokeObjectURL(url),1500);
}
async function reset(){
  resets=true;
  if(loadJob) loadJob.stop=true;
  try {
    await api("/api/experiment/new",{seed:false});
    await refresh();
    report("已复位为空车，A/B/C 已启动，时钟及链路已恢复");
  } finally {resets=false;if(state) renderShopping();}
}
function readRule() {
  if(!state) return;
  const rule=state.network.rules.find(r=>r.source===value("rule-source") && r.target===value("rule-target"));
  $("rule-delay").value=(rule?.delay_ms??state.network.delay_ms)/1000;
  $("rule-copies").value=rule?.copies??state.network.copies;
  $("rule-drop").value=rule?.drop_next??0;
}
const buttons={
  "reset":reset,
  "dispatch-mode":()=>network({manual:!state.network.manual},state.network.manual?"已恢复自动投递":"已暂停，后续消息可逐份投递"),
  "power":()=>power(selected),
  "isolate-node":()=>network({isolate:selected},"已隔离 "+selected+" 的双向链路"),
  "connect-node":()=>network({reconnect:selected},"已接通 "+selected+" 的双向链路"),
  "heal-all":()=>network({heal:true},"已恢复所有链路、投递和后台修复默认值"),
  "lose-ack":async()=>{await api("/api/fault/reply",{node:selected,enabled:!state.drop_reply.includes(selected)});report(selected+" 的确认丢失设置已更新");},
  "apply-clock":()=>clockChange(num("offset")),
  "clear-clock":()=>clockChange(0),
  "set-rule":()=>network({rule:{source:value("rule-source"),target:value("rule-target"),delay_ms:Math.round(num("rule-delay")*1000),
                              copies:num("rule-copies"),drop_next:num("rule-drop")}},"已应用单向传输规则"),
  "clear-rule":async()=>{await network({rule:{source:value("rule-source"),target:value("rule-target"),clear:true}},"已移除此方向的规则");},
  "send-packet":async()=>{const r=await api("/api/dispatch/send",{source:value("send-source"),target:value("send-target")});report("已发送快照组 #"+r.group+"，投递结果见队列");},
  "cas-race":()=>Promise.all(lanes.map(lane=>compare(lane,"write"))),
  "begin-import":async()=>{const r=await api("/api/migration/start",{source:value("import-source"),target:value("import-target"),fresh:true,manual:true});report(`已准备 ${r.total} 片，等待你发送第一片`);},
  "retry-import":async()=>{const m=state.migration;await api("/api/migration/start",{source:m.source,target:m.target,manual:true});report("已重新取得快照，等待逐片发送");},
  "import-power":()=>power(importRoute()[1]),
  "import-link":()=>{const [source,target]=importRoute();return network({link:{source,target,open:blocked(source,target)}},"已更新迁移方向的链路");},
  "run-load":runLoad,
  "stop-load":()=>{if(loadJob) loadJob.stop=true;},
  "capture":capture,
  "export-free":exportRecord
};
async function clockChange(offset) {
  const id=selected;
  await api("/api/fault/clock",{node:id,offset});
  report(`节点 ${id} 的时钟偏移已设为 ${offset} 秒`);
}
async function execute(b) {
  const d=b.dataset;
  if(d.client) return operate(d.client,d.op,d.sku);
  if(d.cas) return compare(d.cas,d.method);
  if(d.edge){
    const [source,target]=d.edge;
    const open=blocked(source,target);
    return network({link:{source,target,open,both:$("both").checked}},`${source} ${$("both").checked?"↔":"→"} ${target} 已${open?"接通":"断开"}`);
  }
  if(d.packet){
    const data={id:Number(d.packet),action:d.adjust};
    if(d.adjust==="delay") data.delay_ms=Math.round(num("packet-delay")*1000);
    const reply=await api("/api/dispatch/packet",data);
    const result=d.adjust==="duplicate"?"已创建副本":d.adjust==="delay"?"已修改到期时间":statusNames[reply.status];
    report(`#${reply.id} · ${result}`);
    return;
  }
  if(d.import){
    await api("/api/migration/control",{job:state.migration.job,action:d.import});
    report(b.textContent+" · 指令已接受");
    return;
  }
  if(buttons[b.id]) await buttons[b.id]();
}
document.addEventListener("click",async event=>{
  const b=event.target.closest("button");
  if(!b||b.disabled) return;
  if(b.dataset.tab) return switchTab(b.dataset.tab);
  if(!state || resets || activeButtons.has(b)) return;
  if(b.dataset.pick){selected=b.dataset.pick;render();return;}
  if(b.dataset.page){page+=Number(b.dataset.page);renderQueue();return;}
  activeButtons.add(b);
  try {await execute(b);}
  catch(e){report(explain(e),true);}
  finally {activeButtons.delete(b);await refresh();}
});
document.addEventListener("change",async event=>{
  if(!state) return;
  const el=event.target;
  try {
    if(el.id==="repair") await network({repair_enabled:el.checked},el.checked?"已开启周期补同步":"已暂停新增周期补同步包");
    if(el.id==="auto-reorder") await network({reorder:el.checked},el.checked?"已开启交替延迟":"已关闭交替延迟");
    if(["rule-source","rule-target"].includes(el.id)) readRule();
    if(el.id==="recipe-choice") renderRecipe();
    if(["show-repair","queue-filter"].includes(el.id)){page=0;renderQueue();}
    render();
  } catch(e){report(explain(e),true);}
  if(["repair","auto-reorder"].includes(el.id)) await refresh();
});
initForms();
refresh();
const stream=new EventSource("/api/events");
stream.addEventListener("change",schedule);
stream.onopen=schedule;
stream.onerror=()=>setText("connection","通知重连中");
setInterval(()=>{if(!document.hidden && !loadJob) refresh();},8000);
setInterval(()=>{if(!document.hidden && active==="packets" && !resets) refreshPackets().catch(()=>{});},1000);
document.addEventListener("visibilitychange",()=>{if(!document.hidden)schedule();});
window.addEventListener("pagehide",()=>stream.close());


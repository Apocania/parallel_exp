"use strict";
// Each step executes real APIs, checks observed state, then returns its evidence.
// Text labelled "expected" never stands in for an observed outcome.
const prepared = (apple=false) => ({
  label:"准备", action:"准备本次实验", detail:"恢复正常网络，启动 A / B / C，使用新的独立购物车。",
  expected:apple?"两端都有牛奶 1 件、苹果 1 件。":"两端都只有牛奶 1 件。",
  run:async(p,c)=>{await p.prepare(c,apple); return "初始商品已在 A、B、C 三个副本一致，之前的故障已解除。";}
});
const step=(label,action,detail,expected,run)=>({label,action,detail,expected,run});
function claimApple(p,c,node,value) {
  return p.api("/api/cas",{node,key:c.cartId+".apple",expected:0,value,cart_id:c.cartId});
}
const normalItems={milk:1};
const CartLabScenes=[
  {id:"normal",name:"正常同步",lead:"先体验没有故障的购物车。",concept:"操作后立即触发复制，另一端收到真实节点的新状态后更新。短暂的网络传输时间仍然存在。",conclusion:"两端操作都能快速传到另一端，无需手动刷新。",steps:[prepared(),
    step("手机添加","手机添加苹果","点击后，手机经 A 添加 1 件苹果，观察右侧电脑购物车。","两端都变为：牛奶 1 件、苹果 1 件。",async(p,c)=>{const t=performance.now();await p.op(c,"add-apple","A","add","apple");await p.waitCart({milk:1,apple:1});c.syncMs=Math.round(performance.now()-t);return `电脑已收到苹果，A/B/C 一致。本步实测约 ${c.syncMs} 毫秒（包含请求与观测时间）。`; }),
    step("电脑移除","电脑移除苹果","电脑经 B 删除刚刚看到的苹果，观察手机。","两端都回到：牛奶 1 件。",async(p,c)=>{await p.remove(c,"remove-apple","B","apple");await p.waitCart(normalItems);return "手机也已移除苹果。正常网络下，双向同步验证通过。";})]},
  {id:"partition",name:"断网合并",lead:"两边暂时不能通信，仍分别加商品。",concept:"分区不等于宕机。A 仍服务手机，B 仍服务电脑；恢复后按 CRDT 合并两边的新增。",conclusion:"断网期间两边都能修改；恢复后，两边新增的商品全部保留。",steps:[prepared(),
    step("断开通信","断开 A 与其他节点","只断开服务器之间的通信，手机与 A 的连接仍然正常。","A/B/C 都在线，连线变为断开；商品暂时仍相同。",async(p)=>{await p.network({isolate:"A"});const s=await p.read();p.assert(s.nodes.filter(n=>"ABC".includes(n.node)).every(n=>n.online),"应该只是断网，节点不应离线。");return "A 仍在线，但 A↔B、A↔C 的同步通道已断开。";}),
    step("分别添加","手机加苹果，电脑加饼干","执行两次独立添加，让你清楚看到两边的差异。","手机：牛奶＋苹果；电脑：牛奶＋饼干。",async(p,c)=>{await Promise.all([p.op(c,"apple","A","add","apple"),p.op(c,"cookie","B","add","cookie")]);const s=await p.read();p.assert(p.qty(s,"A","apple")===1&&p.qty(s,"A","cookie")===0&&p.qty(s,"B","cookie")===1&&p.qty(s,"B","apple")===0,"两边应保留各自新增，且尚未跨分区传播。");return "两次写入均成功，手机与电脑目前各有不同的新增商品。";}),
    step("恢复合并","恢复通信并核对商品","恢复链路，系统立即开始补同步。","两端最终都有：牛奶、苹果、饼干，各 1 件。",async(p)=>{await p.network({heal:true});await p.waitCart({milk:1,apple:1,cookie:1});return "三种商品全部保留，A/B/C 完整状态一致，没有一端覆盖另一端。";})]},
  {id:"conflict",name:"同时加与删",lead:"删掉旧苹果，同时又添加新苹果。",concept:"每次添加都有自己的标记。删除只影响已经观察到的标记，另一端并发产生的新标记会留下。",conclusion:"并发新增会保留；看到新苹果后再次删除，仍然能删干净。",steps:[prepared(true),
    step("制造冲突","断网后：手机删苹果，电脑加苹果","两端原本都看过旧苹果；分区后，一端删旧的，另一端新增一件。","手机没有苹果；电脑暂时有 2 件苹果。",async(p,c)=>{await p.network({isolate:"A"});await Promise.all([p.remove(c,"delete-old","A","apple"),p.op(c,"add-new","B","add","apple")]);const s=await p.read();p.assert(p.qty(s,"A","apple")===0&&p.qty(s,"B","apple")===2,"冲突阶段的苹果数量应为手机 0、电脑 2。");return "手机删除了旧标记；电脑保留旧苹果并增加了一个新标记。";}),
    step("合并冲突","恢复通信，查看剩下的苹果","合并添加记录与删除记录。","两端都剩 1 件苹果，即电脑新添加的那件。",async(p)=>{await p.network({heal:true});await p.waitCart({milk:1,apple:1});return "旧苹果在两端都被删除，新苹果保留，数量为 1。";}),
    step("再次删除","看到新苹果后，再删除一次","这次删除已经观察到了新苹果。","两端都没有苹果，只剩牛奶。",async(p,c)=>{await p.remove(c,"delete-new","A","apple");await p.waitCart(normalItems);return "新苹果也已删除。添加优先不意味着商品永远删不掉。";})]},
  {id:"crash",name:"宕机恢复",lead:"一个服务进程停止，另一个继续工作。",concept:"宕机时 A 无法服务，区别于仍可服务的网络分区。重启后先恢复磁盘数据，再补齐其他节点的新更新。",conclusion:"一个副本宕机不妨碍其他副本写入；恢复后，旧数据和新更新都能保留。",steps:[prepared(),
    step("停止 A","停止节点 A 的进程","真实停止 A，保留它的数据库。","手机显示服务不可用；B、C 继续在线。",async(p)=>{await p.node("A","stop");const s=await p.read();p.assert(!p.getNode(s,"A").online,"A 应已经离线。");return "A 已离线，手机入口不可用；电脑连接的 B 仍在服务。";}),
    step("电脑继续写","电脑添加面包","A 离线期间，经 B 写入一件面包。","电脑有牛奶＋面包，手机仍不可用。",async(p,c)=>{await p.op(c,"bread","B","add","bread");await p.waitCart({milk:1,bread:1},"BC");return "B、C 已保存面包。A 尚未恢复，不能将它标为已同步。";}),
    step("重启补齐","重启 A，检查是否补齐","启动 A 进程，并立即触发恢复同步。","手机重新可用，也有牛奶＋面包。",async(p)=>{await p.node("A","start");await p.waitCart({milk:1,bread:1});return "A 已恢复服务，保留原有牛奶，并补齐宕机期间添加的面包。";})]},
  {id:"duplicate",name:"重复消息",lead:"一件商品对应的同步消息送三遍。",concept:"重复的是同一份状态消息，不是用户独立点击三次。相同添加标记反复合并，不会生成新商品。",conclusion:"同步消息重复送达三次，商品仍只增加一件。",steps:[prepared(),
    step("三重投递","发送三份同步消息，添加 1 件橙子","给每条同步消息复制三份，再由手机添加一次橙子。","消息记录中可见同一组 3 份送达；购物车橙子数量仍为 1。",async(p,c)=>{await p.network({copies:3});const r=await p.op(c,"orange","A","add","orange");await p.waitCart({milk:1,orange:1});await p.waitFor(s=>{const groups=new Map();for(const t of s.transmissions||[]){if(t.source==="A"&&t.target==="B"&&t.copies===3&&t.status==="delivered"&&t.request_ids.includes(r.request_id)){const v=groups.get(t.group)||new Set();v.add(t.copy);groups.set(t.group,v);}}return [...groups.values()].some(v=>v.size===3);},6000,"尚未观测到同一组的三份消息全部送达。");return "已观测到 A→B 同一组消息的第 1、2、3 份全部送达；两端橙子都只有 1 件。";}),
    step("恢复正常","关闭重复投递并复核","恢复正常消息传输，数量应保持不变。","两端仍只有 1 件橙子。",async(p)=>{await p.network({heal:true});await p.waitCart({milk:1,orange:1});return "数量没有重复增加，幂等合并验证通过。";})]},
  {id:"retry",name:"响应丢失",lead:"写入已经成功，但客户端没收到确认。",concept:"超时不等于未执行。重试沿用原请求 ID 和操作内容，服务端才能识别这是同一次添加。",conclusion:"第一次没收到确认，重试也不会多加一件商品。",steps:[prepared(),
    step("丢失确认","添加橙子，但丢弃这次响应","让 A 完成写入后，丢弃返回手机的确认。","手机提示结果未知；节点里的橙子实际上已经是 1 件。",async(p,c)=>{await p.api("/api/fault/reply",{node:"A"});try{await p.op(c,"orange","A","add","orange");throw new Error("没有发生预期的响应丢失。");}catch(e){if(e.status!==504||!e.payload?.ambiguous)throw e;}await p.waitCart({milk:1,orange:1});return "确实收到结果未知响应；实际读取节点后确认，橙子已写入 1 件。";}),
    step("同 ID 重试","用原请求 ID 重试","发送与上一步完全相同的请求 ID 与操作。","提示已去重，橙子数量仍为 1。",async(p,c)=>{const r=await p.op(c,"orange","A","add","orange");p.assert(r.duplicate,"本次必须由服务端识别为重复请求。");await p.waitCart({milk:1,orange:1});return `服务端返回 duplicate=true。原请求 ${r.request_id.slice(0,12)}… 已去重，橙子仍为 1 件。`;})]},
  {id:"delay",name:"延迟与乱序",advanced:true,lead:"旧的添加消息晚到，会不会让已删除的苹果复活？",concept:"同一节点先添加再删除。旧快照延迟约 12 秒，新快照约 10 秒，实际到达顺序由送达时间核对。删除记录参与合并，旧添加不会复活。",conclusion:"新的删除状态先到，旧的添加状态后到，苹果仍保持删除。",steps:[prepared(),
    step("生成两条消息","A 先添加苹果，再立即删除","自动执行这两个操作，确保旧快照已排队再发新快照。旧消息等约 12 秒，新消息等约 10 秒。","消息记录中有添加与删除两次操作；当前两端都无苹果，但 B 还没收到删除记录。",async(p,c)=>{
      await p.network({delay_ms:10000,reorder:true});c.delayStarted=performance.now();
      const added=await p.op(c,"old-add","A","add","apple");
      let s=await p.waitFor(s=>s.operations.some(o=>o.request_id===added.request_id&&o.traces.some(t=>t.source==="A"&&t.target==="B"&&t.reason==="write")),5000,"旧快照尚未排队。");
      c.olderGroup=s.operations.find(o=>o.request_id===added.request_id).traces.find(t=>t.source==="A"&&t.target==="B"&&t.reason==="write").group;
      const removed=await p.remove(c,"new-remove","A","apple");
      s=await p.waitFor(s=>s.operations.some(o=>o.request_id===removed.request_id&&o.traces.some(t=>t.source==="A"&&t.target==="B"&&t.reason==="write")),5000,"新快照尚未排队。");
      c.newerGroup=s.operations.find(o=>o.request_id===removed.request_id).traces.find(t=>t.source==="A"&&t.target==="B"&&t.reason==="write").group;
      p.assert(p.getNode(s,"A").cart.tombstone_count===1&&p.getNode(s,"B").cart.tombstone_count===0,"A 应已有删除记录，B 还没收到。");
      return `旧消息 #${c.olderGroup} 包含苹果；新消息 #${c.newerGroup} 包含删除记录。商品看起来相同，但完整状态还不同。`;
    }),
    step("观察后发先到","等待两条消息，并检查苹果是否复活","保留故障设置，等两条 A→B 消息实际到达；比较送达时间，再核对商品。","新消息先到，旧消息后到；A/B/C 最终都只有牛奶，苹果没有复活。",async(p,c)=>{
      const s=await p.waitFor(s=>{
        const traces=s.operations.flatMap(o=>o.traces);
        return [c.olderGroup,c.newerGroup].every(g=>traces.some(t=>t.group===g&&t.source==="A"&&t.target==="B"&&t.status==="delivered"));
      },35000,"两条目标消息尚未全部送达。");
      const traces=s.operations.flatMap(o=>o.traces),old=traces.find(t=>t.group===c.olderGroup&&t.target==="B"),fresh=traces.find(t=>t.group===c.newerGroup&&t.target==="B");
      p.assert(old.delivered_at>fresh.delivered_at,"本轮未实际出现预期的后发先到，请重新实验。");
      await p.waitCart(normalItems);await p.network({heal:true});
      return `已核对送达时间：新消息 #${c.newerGroup} 先到，旧消息 #${c.olderGroup} 后到。苹果仍为 0，删除记录阻止了复活；网络已恢复。`;
    })]},
  {id:"clock",name:"时钟回拨",advanced:true,lead:"机器时间倒退，逻辑版本继续向前。",concept:"本实现用逻辑计数记录版本，不用物理时间决定哪个购物车覆盖另一个。",conclusion:"物理时间回拨不会使版本计数倒退。",steps:[prepared(),
    step("回拨并写入","A 的时间倒退一分钟，再添加苹果","保留回拨前的时间和逻辑计数，再写入一次。","节点 A 的展示时间倒退，版本计数递增；商品仍正常同步。",async(p,c)=>{const before=p.getNode(await p.read(),"A");c.clockBefore=before;await p.api("/api/fault/clock",{node:"A",offset:-60});await p.op(c,"apple","A","add","apple");await p.waitCart({milk:1,apple:1});const after=p.getNode(await p.read(),"A");p.assert(after.wall_time<before.wall_time-50&&after.cart.vv[after.actor]>before.cart.vv[before.actor],"应同时观察到时间倒退与逻辑计数递增。");return `A 的时钟偏移为 ${after.clock_offset} 秒；本地逻辑计数 ${before.cart.vv[before.actor]} → ${after.cart.vv[after.actor]}，苹果已同步。`;}),
    step("恢复时钟","恢复 A 的展示时间","仅恢复实验时钟，不改变商品内容。","两端仍都有牛奶和苹果。",async(p)=>{await p.api("/api/fault/clock",{node:"A",offset:0});await p.waitCart({milk:1,apple:1});return "时钟已恢复，购物车内容未改变。";})]},
  {id:"cas",name:"CAS 对照",advanced:true,lead:"同一个领苹果名额，两人同时领，购物车里会有几个？",concept:"每个节点独立检查领取版本。成功时在同一个本地事务中记录领取人、添加一个苹果。联网后购物车合并所有成功发放的苹果。",conclusion:"同节点发出 1 个苹果；分区两侧各发 1 个，合并后购物车有 2 个。",steps:[
    step("准备","准备空购物车","手机和电脑共用本轮购物车，领取版本从 0 开始。","三个副本的购物车都为空。",async(p,c)=>{
      await p.prepare(c,false,false);
      return "空购物车已就绪，可以开始领取苹果。";
    }),
    step("同节点领取","手机和电脑都向 A 领苹果","两人使用同一个领取名额，都提交期望版本 0。","一人领到，一人遇到版本冲突，购物车显示苹果 ×1。",async(p,c)=>{
      p.showClaim("phone","A","向 A 领取苹果…");
      p.showClaim("desktop","A","向 A 领取苹果…");
      const results=await Promise.allSettled(["手机","电脑"].map(value=>claimApple(p,c,"A",value)));
      const codes=results.map(r=>r.status==="fulfilled" && r.value.swapped ? 200 : r.reason?.status);
      p.assert(codes.includes(200) && codes.includes(409),"同节点应有一次领取成功和一次版本冲突。");
      ["phone","desktop"].forEach((client,i)=>p.showClaim(client,"A",codes[i]===200?"HTTP 200 · 领到 1 个苹果":"HTTP 409 · 这个名额已被领取"));
      await p.waitCart({apple:1});
      return `手机 HTTP ${codes[0]}，电脑 HTTP ${codes[1]}。三个副本都读到苹果 ×1。`;
    }),
    step("分区领取","换一辆空车，分别向 A、B 领取","新一轮从零开始。隔离 A，手机经 A、电脑经 B 领取同一名额。","两人都领取成功。网络恢复前，手机和电脑各显示苹果 ×1。",async(p,c)=>{
      await p.prepare(c,false,false);
      await p.network({isolate:"A"});
      const results=await Promise.all([claimApple(p,c,"A","手机"),claimApple(p,c,"B","电脑")]);
      p.assert(results.every(r=>r.swapped),"两端应各自领取成功。");
      p.showClaim("phone","A","HTTP 200 · 从 A 领到 1 个苹果");
      p.showClaim("desktop","B","HTTP 200 · 从 B 领到 1 个苹果");
      const state=await p.read();
      p.assert(p.qty(state,"A","apple")===1 && p.qty(state,"B","apple")===1,"分区期间两端应各看到一个苹果。");
      return "两笔领取都成功。手机上的苹果来自 A，电脑上的苹果来自 B。";
    }),
    step("恢复合并","恢复网络，查看苹果数量","让两个节点交换购物车记录。","同一辆车的两端都显示苹果 ×2。",async(p)=>{
      await p.network({heal:true});
      await p.waitCart({apple:2});
      return "A、B、C 都读到苹果 ×2。两次领取各产生一个苹果，合并后都保留下来。";
    })]},
  {id:"migration",name:"迁移中断",advanced:true,lead:"新节点只收到一半数据时，不能冒充完整副本。",concept:"数据先进入暂存区，全部收到且校验成功后才开放服务。实验会停在一半，方便你真正执行宕机。",conclusion:"不完整副本不会对外服务；重试完整导入后才能加入同步。",steps:[prepared(),
    step("导入一半","新建 D，并在导入一半时暂停","创建新一轮 D 的独立数据库，保留旧实验数据。","D 显示导入中，进度停在 4/8，尚不可服务。",async(p)=>{await p.api("/api/migration/start",{fresh:true,pause_after:4});await p.waitFor(s=>s.migration?.status==="paused",12000,"导入尚未到达中途检查点。");const s=await p.read();p.assert(!p.getNode(s,"D").ready,"D 尚不应开放服务。");return `D 已收到 ${s.migration.sent}/${s.migration.total} 片；刻意暂停在中途，等你执行下一步。`;}),
    step("中途宕机","停止 D，并检查部分数据是否可用","停止真实进程，再启动它，检查是否错误地开放服务。","D 重启后仍处于导入中，普通写入会被拒绝。",async(p,c)=>{await p.node("D","stop");await p.waitFor(s=>s.migration?.status==="interrupted",5000,"尚未记录迁移中断。");await p.node("D","start");p.assert(!p.getNode(await p.read(),"D").ready,"部分快照不能作为完整副本。");try{await p.op(c,"probe","D","add","apple");throw new Error("未完成导入的节点错误地接受了写入。");}catch(e){if(e.status!==503)throw e;}return "D 从暂存状态恢复，仍拒绝普通写入，没有暴露半份购物车。";}),
    step("重新导入","重新导入并加入同步","重新获取完整快照、校验并提交，再检查四个节点。","D 开始服务，A/B/C/D 都有牛奶 1 件。",async(p)=>{await p.api("/api/migration/start",{});await p.waitFor(s=>s.migration?.status==="complete",15000,"导入还未完成。");await p.waitCart(normalItems,"ABCD");return "完整快照已提交，D 已开放服务，四个节点状态一致。";})]}
];

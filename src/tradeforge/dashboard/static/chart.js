"use strict";
/* Original local canvas renderer. No network, dependencies or synthetic prices. */
function candles(rows) {
  if (!Array.isArray(rows)) return [];
  let last = -Infinity;
  return rows.slice(-300).flatMap(row => {
    const b = Object.fromEntries(["time","open","high","low","close","volume"].map(k => [k, Number(row[k])]));
    if (Object.values(b).some(x => !Number.isFinite(x)) || b.time <= last || b.low <= 0 || b.volume < 0 || b.low > Math.min(b.open,b.close) || b.high < Math.max(b.open,b.close)) return [];
    last = b.time; return [b];
  });
}
function viewport(rows, count, offset) {
  count = Math.max(15, Math.min(300, Math.round(count)));
  offset = Math.max(0, Math.min(Math.max(0,rows.length - count), Math.round(offset)));
  return rows.slice(Math.max(0,rows.length-offset-count), rows.length-offset);
}
function compatibleLevels(levels, environment, symbol) {
  return (levels || []).filter(row => row.environment === environment && row.symbol === symbol && Number.isFinite(Number(row.price)) && Number(row.price) > 0).slice(0,20);
}
class TerminalChart {
  constructor(canvas, empty) {
    this.canvas = canvas; this.empty = empty; this.rows = []; this.count = 80; this.offset = 0; this.cursor = null; this.levels = [];
    this.resize = new ResizeObserver(() => this.draw()); this.resize.observe(canvas.parentElement);
    canvas.addEventListener("wheel", e => {e.preventDefault(); this.count = Math.max(15, Math.min(300,this.count*(e.deltaY>0?1.15:0.85))); this.draw();}, {passive:false});
    canvas.addEventListener("pointerdown", e => {this.drag = [e.clientX,this.offset]; canvas.setPointerCapture(e.pointerId);});
    canvas.addEventListener("pointerup", () => {this.drag = null;});
    canvas.addEventListener("pointermove", e => {const box = canvas.getBoundingClientRect(); this.cursor=[e.clientX-box.left,e.clientY-box.top]; if(this.drag){this.offset=Math.max(0,Math.min(Math.max(0,this.rows.length-this.count),this.drag[1]+(this.drag[0]-e.clientX)/6));} this.draw();});
    canvas.addEventListener("pointerleave", () => {this.cursor=null; this.draw();});
  }
  set(data, levels=[]) {const key=data.symbol+":"+data.timeframe+":"+data.environment;if(key!==this.key){this.offset=0;this.key=key;}this.rows=candles(data.bars);this.levels=compatibleLevels(levels,data.environment,data.symbol);this.draw();}
  draw() {
    const box=this.canvas.parentElement.getBoundingClientRect(), w=Math.max(200,Math.min(2400,box.width)), h=Math.max(220,Math.min(1000,box.height)), d=Math.min(2,window.devicePixelRatio||1);
    if(this.canvas.width!==Math.round(w*d)||this.canvas.height!==Math.round(h*d)){this.canvas.width=w*d;this.canvas.height=h*d;this.canvas.style.width=w+"px";this.canvas.style.height=h+"px";}
    const c=this.canvas.getContext("2d"); c.setTransform(d,0,0,d,0,0); c.clearRect(0,0,w,h);
    this.empty.hidden=this.rows.length>0; if(!this.rows.length) return;
    const rows=viewport(this.rows,this.count,this.offset), right=w-76, bottom=h-42, priceBottom=bottom*0.78, top=20;
    let lo=Math.min(...rows.map(b=>b.low)), hi=Math.max(...rows.map(b=>b.high)), range=hi-lo || hi*0.001; lo-=range*0.08; hi+=range*0.08;
    const y=p=>top+(hi-p)/(hi-lo)*(priceBottom-top), step=(right-12)/rows.length, x=i=>12+(i+0.5)*step;
    c.font="11px ui-monospace,monospace"; c.lineWidth=1;
    for(let i=0;i<5;i++){const yy=top+i*(priceBottom-top)/4; c.strokeStyle="#202735"; c.beginPath(); c.moveTo(0,yy); c.lineTo(right,yy); c.stroke(); c.fillStyle="#7d899e"; c.fillText((hi-i*(hi-lo)/4).toLocaleString("en-US",{maximumSignificantDigits:7}),right+10,yy+4);}
    const maxVolume=Math.max(...rows.map(b=>b.volume),1);
    rows.forEach((b,i)=>{const xx=x(i), bw=Math.max(1,Math.min(12,step*0.66)), up=b.close>=b.open; c.strokeStyle=c.fillStyle=up?"#26c6a0":"#ee6b7d"; c.beginPath(); c.moveTo(xx,y(b.high)); c.lineTo(xx,y(b.low)); c.stroke(); c.fillRect(xx-bw/2,Math.min(y(b.open),y(b.close)),bw,Math.max(1,Math.abs(y(b.open)-y(b.close)))); c.globalAlpha=0.28; c.fillRect(xx-bw/2,bottom-(b.volume/maxVolume)*(bottom-priceBottom-16),bw,(b.volume/maxVolume)*(bottom-priceBottom-16)); c.globalAlpha=1;});
    for(let i=0;i<4;i++){const n=Math.round(i*(rows.length-1)/3); const text=new Date(rows[n].time*1000).toISOString().slice(5,16).replace("T"," "); c.fillStyle="#7d899e"; c.fillText(text,Math.max(4,Math.min(right-100,x(n)-40)),h-14);}
    this.levels.forEach(level=>{const yy=y(Number(level.price)); if(yy<top||yy>priceBottom)return; c.setLineDash([5,4]); c.strokeStyle="#e8bb6a"; c.beginPath(); c.moveTo(0,yy); c.lineTo(right,yy); c.stroke(); c.setLineDash([]); c.fillStyle="#e8bb6a"; c.fillText(level.kind+" "+level.price,12,yy-5);});
    if(this.cursor&&this.cursor[0]<right){const [cx,cy]=this.cursor; c.strokeStyle="#8997ae"; c.setLineDash([3,4]); c.beginPath(); c.moveTo(cx,top); c.lineTo(cx,bottom); c.moveTo(0,cy); c.lineTo(right,cy); c.stroke(); c.setLineDash([]); const b=rows[Math.max(0,Math.min(rows.length-1,Math.floor((cx-12)/step)))]; c.fillStyle="#d6deee"; c.fillText(`O ${b.open}   H ${b.high}   L ${b.low}   C ${b.close}   V ${b.volume}`,12,12);}
  }
}
if(typeof module!=="undefined")module.exports={candles,viewport,compatibleLevels,TerminalChart};
if(typeof window!=="undefined")window.TerminalChart=TerminalChart;

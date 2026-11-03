export type PlatformAvailability={authenticated:boolean;reachability:'unknown'|'online'|'offline';checkedAt?:string|null;lastFailure?:unknown};
export function platformStatus(value?:PlatformAvailability,legacyAuthenticated=false){
  const authenticated=value?.authenticated??legacyAuthenticated;
  if(!authenticated)return {connected:false,authenticated:false,label:'尚未登录'};
  if(value?.reachability==='online')return {connected:true,authenticated:true,label:'已连接'};
  return {connected:false,authenticated:true,label:value?.reachability==='offline'?'平台暂不可达 · 登录已保留':'已登录 · 连接待核实'};
}

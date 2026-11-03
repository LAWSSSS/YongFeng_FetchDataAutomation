const eventName='media-state-changed';
let revision=0;
const relevant=/^\/api\/(?:videos(?:\/|$)|video-processing(?:\/|$)|pipeline(?:\/|$)|slicing(?:\/|$)|collection-runs(?:\/|$)|jobs(?:\/|$))/;

/** An accepted command invalidates reads; it never predicts a worker result. */
export function announceMediaChange(path:string,target?:EventTarget){
  if(!relevant.test(path))return;
  revision++;
  const destination=target??(typeof window!=='undefined'?window:undefined);
  destination?.dispatchEvent(new Event(eventName));
}
export const mediaRevision=()=>revision;
export function onMediaChange(target:EventTarget,refresh:()=>void){
  target.addEventListener(eventName,refresh);
  return ()=>target.removeEventListener(eventName,refresh);
}

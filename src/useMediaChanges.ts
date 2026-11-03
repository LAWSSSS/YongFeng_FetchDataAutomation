import {useEffect,useRef} from 'react';
import {onMediaChange} from './media-updates';

export function useMediaChanges(refresh:()=>unknown){
  const latest=useRef(refresh);latest.current=refresh;
  useEffect(()=>onMediaChange(window,()=>{void latest.current();}),[]);
}

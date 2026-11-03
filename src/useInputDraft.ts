import {useCallback,useRef,useState} from 'react';
import type {SetStateAction} from 'react';
import {readDraft,writeDraft} from './draft-storage';

/** Persist before navigation can unmount an editor; stale version metadata stays intact. */
export function useInputDraft<T>(key:string,fallback:T,valid:(value:unknown)=>value is T){
  const [value,setValue]=useState(()=>readDraft(key,fallback,valid));
  const latest=useRef(value);
  const update=useCallback((action:SetStateAction<T>)=>{
    const next=typeof action==='function'?(action as (before:T)=>T)(latest.current):action;
    latest.current=next;writeDraft(key,next);setValue(next);
  },[key]);
  return [value,update] as const;
}

import { createContext, useContext, useEffect, useRef, useState } from 'react';
import type { ReactNode } from 'react';
import { CaretRight, CalendarBlank } from '@phosphor-icons/react';
import {viewMemory} from './view-memory';
import './date-records.css';

const DateSectionOpen=createContext(true);
export const useDateSectionOpen=()=>useContext(DateSectionOpen);

export default function DateSection({day,summary,children,expandedKey,rememberKey}:{day:string;summary:ReactNode;children:ReactNode;expandedKey?:string;rememberKey?:string}) {
  const restored=useRef(rememberKey?viewMemory.read(`date:${rememberKey}`,false):false);
  const [visited,setVisited]=useState(restored.current);
  const [expanded,setExpanded]=useState(restored.current);
  const element=useRef<HTMLDetailsElement>(null);
  useEffect(()=>{if(restored.current&&element.current)element.current.open=true;},[]);
  useEffect(()=>{if(expandedKey){setVisited(true);setExpanded(true);if(element.current)element.current.open=true;}},[expandedKey]);
  return <details ref={element} className="date-section" onToggle={event=>{
    if(event.target!==event.currentTarget)return;
    setExpanded(event.currentTarget.open);if(event.currentTarget.open)setVisited(true);
    if(rememberKey)viewMemory.save(`date:${rememberKey}`,event.currentTarget.open);
  }}>
    <summary><CalendarBlank className="date-icon" size={20} aria-hidden="true"/>
      <div className="date-heading"><strong>{day || '日期未记录'}</strong><span>{summary}</span></div>
      <span className="date-expand">{expanded?'收起':'查看记录'}</span><CaretRight className="date-chevron" size={18} aria-hidden="true"/>
    </summary>
    {visited&&<DateSectionOpen value={expanded}><div className="date-content">{children}</div></DateSectionOpen>}
  </details>;
}

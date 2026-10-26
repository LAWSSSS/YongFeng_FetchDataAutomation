import { useState } from 'react';
import type { ReactNode } from 'react';
import { CaretRight, CalendarBlank } from '@phosphor-icons/react';
import './date-records.css';

export default function DateSection({day,summary,children}:{day:string;summary:ReactNode;children:ReactNode}) {
  const [visited,setVisited]=useState(false);
  return <details className="date-section" onToggle={event=>{if(event.currentTarget.open)setVisited(true);}}>
    <summary><CalendarBlank className="date-icon" size={20} aria-hidden="true"/>
      <div className="date-heading"><strong>{day || '日期未记录'}</strong><span>{summary}</span></div>
      <span className="date-expand">查看记录</span><CaretRight className="date-chevron" size={18} aria-hidden="true"/>
    </summary>
    {visited&&<div className="date-content">{children}</div>}
  </details>;
}

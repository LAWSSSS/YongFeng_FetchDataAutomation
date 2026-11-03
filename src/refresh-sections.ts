type Section={label:string;read:(current:()=>boolean)=>Promise<void>};

export function stateSection<T>(label:string,load:()=>Promise<T>,apply:(value:T)=>void):Section {
  return {label,async read(current){const value=await load();if(current())apply(value);}};
}

/** Independent panels become usable as soon as their own response arrives. */
export async function refreshSections(sections:Section[],current:()=>boolean,onErrors:(errors:string[])=>void) {
  const errors=new Map<string,string>();
  await Promise.all(sections.map(async section=>{
    try {await section.read(current);}
    catch(error){errors.set(section.label,`${section.label}：${error instanceof Error?error.message:'读取失败'}`);}
    finally {if(current())onErrors([...errors.values()]);}
  }));
}

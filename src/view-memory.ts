/** Small, in-memory view preferences only; never stores task data or drafts. */
export function createViewMemory(limit=100) {
  const values=new Map<string,unknown>();
  return {
    read<T>(key:string,fallback:T):T {return values.has(key)?values.get(key) as T:fallback;},
    save<T>(key:string,value:T) {
      values.delete(key);values.set(key,value);
      while(values.size>limit)values.delete(values.keys().next().value!);
    },
  };
}

export const viewMemory=createViewMemory();

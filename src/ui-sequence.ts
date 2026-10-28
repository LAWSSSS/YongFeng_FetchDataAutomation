/** A delayed poll cannot replace a newer operation result. */
export function uiSequence(){
  let serial=0,mutating=false;
  return {
    read(){return mutating?null:++serial;},
    accepts(token:number){return !mutating&&token===serial;},
    begin(){if(mutating)return false;mutating=true;++serial;return true;},
    finish(){++serial;mutating=false;},
    invalidate(){++serial;},
  };
}

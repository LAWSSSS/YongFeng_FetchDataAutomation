import hashlib,json,os
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import cv2
import numpy as np
ROOT=Path(__file__).resolve().parents[1]
SIZE=(160,192)

def inside(path):
 p=Path(path)
 if not p.is_absolute():p=ROOT/p
 p=p.resolve()
 if ROOT not in p.parents: raise ValueError('Path must stay inside execution project')
 return p

def read_image(path):
 image=cv2.imdecode(np.frombuffer(inside(path).read_bytes(),np.uint8),cv2.IMREAD_COLOR)
 if image is None:raise ValueError('Invalid image: '+str(path))
 if image.shape[:2]!=(2160,3840): raise ValueError('Image dimensions differ from reference')
 return image

def polygon_mask(regions,shape=(540,960),scale=.25):
 mask=np.zeros(shape,np.uint8)
 for points in regions:
  p=np.rint(np.asarray(points)*scale).astype(np.int32)
  cv2.fillPoly(mask,[p],255)
 return mask

def alignment_image(image):
 g=cv2.cvtColor(image,cv2.COLOR_BGR2GRAY).astype(np.float32)/255
 g=cv2.GaussianBlur(g,(5,5),0)
 x=cv2.Sobel(g,cv2.CV_32F,1,0);y=cv2.Sobel(g,cv2.CV_32F,0,1)
 mag=cv2.magnitude(x,y)
 return np.minimum(mag,0.5)*2

def align_frame(image,template,wall_mask):
 small=cv2.resize(image,(960,540),interpolation=cv2.INTER_AREA)
 warp=np.eye(2,3,dtype=np.float32); correlation=-1.;reason='ok'
 try:
  x,y,w,h=cv2.boundingRect(wall_mask)
  x0=max(0,x-12);y0=max(0,y-12);x1=min(960,x+w+12);y1=min(540,y+h+12)
  view=np.s_[y0:y1,x0:x1]
  correlation,warp=cv2.findTransformECC(template[view],alignment_image(small)[view],warp,cv2.MOTION_TRANSLATION,
   (cv2.TERM_CRITERIA_EPS|cv2.TERM_CRITERIA_COUNT,60,1e-5),inputMask=wall_mask[view],gaussFiltSize=5)
  if not np.isfinite(warp).all() or np.max(np.abs(warp[:,2]))>20:
   reason='excessive_shift';warp=np.eye(2,3,dtype=np.float32)
  elif correlation<.18:reason='weak_wall_alignment'
 except cv2.error:
  reason='alignment_failed';warp=np.eye(2,3,dtype=np.float32)
 aligned=cv2.warpAffine(small,warp,(960,540),flags=cv2.INTER_LINEAR|cv2.WARP_INVERSE_MAP,borderMode=cv2.BORDER_REFLECT)
 return aligned,{'correlation':round(float(correlation),5),'dx_pixels':round(float(warp[0,2]*4),3),'dy_pixels':round(float(warp[1,2]*4),3),'quality':reason}

def crop_region(aligned,mask,bounds):
 x,y,w,h=bounds
 crop=aligned[y:y+h,x:x+w].copy();local=mask[y:y+h,x:x+w]
 crop[local==0]=0
 crop=cv2.resize(crop,SIZE,interpolation=cv2.INTER_AREA)
 m=cv2.resize(local,SIZE,interpolation=cv2.INTER_NEAREST)
 crop[m==0]=0
 return cv2.cvtColor(crop,cv2.COLOR_BGR2RGB),m

def preprocess(manifest_path,output_dir):
 manifest_path=inside(manifest_path);output_dir=inside(output_dir);output_dir.mkdir(parents=True,exist_ok=True)
 cache=output_dir/'preprocessed.npz';metadata=output_dir/'preprocessing.json'
 if cache.exists() or metadata.exists():raise FileExistsError('Preprocessing output already exists; preserve it')
 m=json.loads(manifest_path.read_text());region=m['regions']
 wall_mask=polygon_mask(region['wall_reference']); wall_mask=cv2.erode(wall_mask,np.ones((3,3),np.uint8))
 roi_mask=polygon_mask(region['vehicle_detection']);roi_mask=cv2.erode(roi_mask,np.ones((3,3),np.uint8))
 bounds=cv2.boundingRect(roi_mask)
 source=next(s for s in m['samples'] if s['filename'].startswith('20260731_165100_1_19064_0_0_0033'))
 template=alignment_image(cv2.resize(read_image(source['path']),(960,540),interpolation=cv2.INTER_AREA))
 crops=[];alignment=[]
 previews=output_dir/'previews';previews.mkdir();rois=output_dir/'roi_previews';rois.mkdir()
 def process(s):
  image=read_image(s['path']);aligned,q=align_frame(image,template,wall_mask)
  crop,mask=crop_region(aligned,roi_mask,bounds)
  cv2.imencode('.jpg',cv2.resize(image,(960,540)),[cv2.IMWRITE_JPEG_QUALITY,84])[1].tofile(str(previews/(s['id']+'.jpg')))
  cv2.imencode('.jpg',cv2.cvtColor(crop,cv2.COLOR_RGB2BGR),[cv2.IMWRITE_JPEG_QUALITY,92])[1].tofile(str(rois/(s['id']+'.jpg')))
  return crop,mask,q
 with ThreadPoolExecutor(max_workers=4) as pool:
  for i,(crop,mask,q) in enumerate(pool.map(process,m['samples'])):
   crops.append(crop);alignment.append(q)
   if (i+1)%250==0:print('PREPROCESSED',i+1,flush=True)
 np.savez_compressed(cache,crops=np.stack(crops),mask=mask,template=template,wall_mask=wall_mask,roi_mask=roi_mask,bounds=np.asarray(bounds))
 meta={'manifest_sha256':hashlib.sha256(manifest_path.read_bytes()).hexdigest(),'image_ids':[s['id'] for s in m['samples']],
  'input_size':list(SIZE),'full_resolution':[3840,2160],'downsample':4,'bounds':list(bounds),'alignment':alignment,
  'reference_annotation_version':m['reference_annotation_version'],'reference_annotation_sha256':m['reference_annotation_sha256'],
  'geometry':'ECC translation from wall reference only; classification pixels strictly inside vehicle detection polygon',
  'crops_sha256':hashlib.sha256(cache.read_bytes()).hexdigest()}
 metadata.write_text(json.dumps(meta,ensure_ascii=False,indent=2))
 print('PREPROCESS_COMPLETE',len(crops),flush=True)
 return cache,metadata

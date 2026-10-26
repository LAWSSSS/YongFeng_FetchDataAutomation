"""Small ROI-only supervised scene classifier; no external or legacy learned weights."""
import math
import cv2
import numpy as np
import torch
from torch import nn
from torch.nn import functional as F
from training.data import polygon_mask, alignment_image, align_frame, crop_region


class Residual(nn.Module):
    def __init__(self,cin,cout):
        super().__init__()
        self.main=nn.Sequential(nn.Conv2d(cin,cout,3,2,1,bias=False),nn.GroupNorm(4,cout),nn.ReLU(),
                                nn.Conv2d(cout,cout,3,1,1,bias=False),nn.GroupNorm(4,cout))
        self.skip=nn.Sequential(nn.Conv2d(cin,cout,1,2,bias=False),nn.GroupNorm(4,cout))
    def forward(self,x):return F.relu(self.main(x)+self.skip(x))


class PresenceNet(nn.Module):
    def __init__(self,input_pool=2):
        super().__init__()
        if input_pool not in (1,2):raise ValueError('input_pool must be 1 or 2')
        self.input_pool=input_pool
        self.backbone=nn.Sequential(Residual(3,8),Residual(8,16),Residual(16,32),Residual(32,64))
        self.head=nn.Sequential(nn.Flatten(),nn.Linear(64*3*3,64),nn.ReLU(),nn.Dropout(.25),nn.Linear(64,1))
    def forward(self,x):
        if self.input_pool>1:x=F.avg_pool2d(x,self.input_pool)
        return self.head(F.adaptive_avg_pool2d(self.backbone(((x-.5)/.25).contiguous()),(3,3))).squeeze(1)


def calibrated_regions(record):
    q=record['source']['alignment_translation_pixels']
    return {role:[[[round(x-q['dx_pixels'],3),round(y-q['dy_pixels'],3)] for x,y in poly] for poly in polygons]
            for role,polygons in record['regions'].items()}


def make_geometry(regions,reference_image):
    wall=cv2.erode(polygon_mask(regions['wall_reference']),np.ones((3,3),np.uint8))
    roi=polygon_mask(regions['vehicle_detection'])
    template=alignment_image(cv2.resize(reference_image,(960,540),interpolation=cv2.INTER_AREA))
    return {'wall_mask':wall,'roi_mask':roi,'bounds':np.asarray(cv2.boundingRect(roi)),'template':template}


def prepare_image(image,geometry):
    aligned,q=align_frame(image,geometry['template'],geometry['wall_mask'])
    crop,mask=crop_region(aligned,geometry['roi_mask'],tuple(int(v) for v in geometry['bounds']))
    return crop,mask,q


def photometric(x,mask):
    n=x.shape[0]
    gray=(x[:,0:1]*.299+x[:,1:2]*.587+x[:,2:3]*.114)
    use_gray=(torch.rand(n,1,1,1,device=x.device)<.35).to(x.dtype)
    y=x*(1-use_gray)+gray*use_gray
    contrast=.8+.4*torch.rand(n,1,1,1,device=x.device)
    y=(y-.5)*contrast+.5
    y=y*(.75+.5*torch.rand(n,1,1,1,device=x.device))
    y=y.clamp(0,1).pow(.75+.5*torch.rand(n,1,1,1,device=x.device))
    y=(y+torch.randn_like(y)*.008).clamp(0,1)
    return y*mask


def threshold_from_validation(labels,scores):
    labels=np.asarray(labels,dtype=np.int64);scores=np.asarray(scores,dtype=np.float64)
    if set(labels.tolist())!={0,1}:raise ValueError('Validation needs both classes')
    points=sorted(set([0.,1.,*scores.tolist()]));candidates=[0.,1.,*[(a+b)/2 for a,b in zip(points,points[1:])]]
    def rank(t):
        m=metrics(labels,scores,t)
        return (m['balanced_accuracy'],m['f1_occupied'],-abs(t-.5))
    return float(max(candidates,key=rank))


def wilson(success,total):
    if total==0:return [None,None]
    z=1.959963984540054;p=success/total;den=1+z*z/total
    center=(p+z*z/(2*total))/den
    radius=z*math.sqrt(p*(1-p)/total+z*z/(4*total*total))/den
    return [max(0.,center-radius),min(1.,center+radius)]


def metrics(labels,scores,threshold=.5):
    y=np.asarray(labels,dtype=np.int64);p=np.asarray(scores)>threshold
    tp=int(((y==1)&p).sum());tn=int(((y==0)&~p).sum());fp=int(((y==0)&p).sum());fn=int(((y==1)&~p).sum())
    safe=lambda a,b:a/b if b else 0.
    accuracy=safe(tp+tn,len(y));precision=safe(tp,tp+fp);recall=safe(tp,tp+fn);specificity=safe(tn,tn+fp)
    return {'images':len(y),'true_occupied':int((y==1).sum()),'true_empty':int((y==0).sum()),
            'accuracy':accuracy,'balanced_accuracy':(recall+specificity)/2,'precision_occupied':precision,
            'recall_occupied':recall,'recall_empty':specificity,'f1_occupied':safe(2*precision*recall,precision+recall),
            'confusion':{'true_empty_pred_empty':tn,'true_empty_pred_occupied':fp,'true_occupied_pred_empty':fn,'true_occupied_pred_occupied':tp},
            'accuracy_wilson_95':wilson(tp+tn,len(y)),'occupied_recall_wilson_95':wilson(tp,tp+fn)}


def recall_selection_rank(result,validation_loss,minimum_accuracy):
    feasible=result['accuracy']+1e-12>=minimum_accuracy
    if feasible:
        return (1.,result['recall_occupied'],result['f1_occupied'],result['balanced_accuracy'],-float(validation_loss))
    return (0.,result['balanced_accuracy'],result['f1_occupied'],result['recall_occupied'],-float(validation_loss))


def threshold_for_recall(labels,scores,minimum_accuracy):
    labels=np.asarray(labels,dtype=np.int64);scores=np.asarray(scores,dtype=np.float64)
    if set(labels.tolist())!={0,1} or not np.isfinite(scores).all():raise ValueError('Need finite validation scores and both classes')
    if not 0<=minimum_accuracy<=1:raise ValueError('Minimum validation accuracy must be in [0,1]')
    points=sorted(set([0.,1.,*scores.tolist()]))
    candidates=[0.,1.,*[(a+b)/2 for a,b in zip(points,points[1:])]]
    threshold=max(candidates,key=lambda t:(*recall_selection_rank(metrics(labels,scores,t),0.,minimum_accuracy),-abs(t-.5)))
    result=metrics(labels,scores,threshold)
    return float(threshold),result

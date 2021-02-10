import warnings
from dataclasses import dataclass, asdict
import torch
from src.proto.runtime_pb2 import CompressionType

warnings.filterwarnings("ignore", "CUDA initialization*", category=UserWarning)


@dataclass(init=True, repr=True, frozen=True)
class DescriptorBase:
    pass


@dataclass(init=True, repr=True, frozen=True)
class TensorDescriptor(DescriptorBase):
    size: tuple
    dtype: torch.dtype = None
    layout: torch.layout = torch.strided
    device: torch.device = None
    requires_grad: bool = False
    pin_memory: bool = False
    compression: CompressionType = CompressionType.NONE

    @property
    def shape(self):
        return self.size

    @classmethod
    def from_tensor(cls, tensor: torch.Tensor):
        return cls(tensor.shape, tensor.dtype, tensor.layout, tensor.device, tensor.requires_grad,
                   safe_check_pinned(tensor))

    def make_empty(self, **kwargs):
        properties = asdict(self)
        properties.update(kwargs)
        return torch.empty(**properties)


def safe_check_pinned(tensor: torch.Tensor) -> bool:
    try:
        return torch.cuda.is_available() and tensor.is_pinned()
    except RuntimeError:
        return False

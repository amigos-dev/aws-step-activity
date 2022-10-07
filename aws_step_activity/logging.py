# Copyright (c) 2022 Amigos Development Inc.
#
# MIT License - See LICENSE file accompanying this package.
#

"""A lingle python logger instance that is shared by all modules in this package"""

import logging
logger = logging.getLogger(__name__.rsplit('.', 1)[0])

__all__ = [ 'logger' ]

class NotValidKeywordError(Exception):
    def __init__(self, msg='Keyword for get reviews is not valid.'):
        self.msg = msg

    def __str__(self):
        return 'NotValidKeywordError: ' + self.msg
    
class NotEnoughSearchVolumeError(Exception):
    def __init__(self, msg='Search volume data is too small. Forecasting is not conducted.'):
        self.msg = msg
    
    def __str__(self):
        return 'NotEnoughSearchVolumeError: '+ self.msg

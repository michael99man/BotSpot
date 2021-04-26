class ChungusSet(Dataset):
    def __init__(self, words, subs, karmas, mods, labels):
        self.words = words
        self.subs = subs
        self.labels = labels
        self.karmas = karmas
        self.mods = mods
        assert(len(words)== len(subs) and len(subs) == len(labels) and len(subs) == len(karmas) and len(mods) == len(karmas))
    
    def __len__(self):
        return len(self.words)

    def __getitem__(self, idx):
        #get images and labels here 
        #returned images must be tensor
        #labels should be int 
        return self.words[idx], self.subs[idx] , self.karmas[idx] ,self.mods[idx],  self.labels[idx] 
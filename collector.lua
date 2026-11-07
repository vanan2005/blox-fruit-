--[[
    BLOX FRUITS GAME DATA COLLECTOR 2.0
    Read-only, standalone Luau data explorer / snapshot exporter.
    NOT an andepzai update. No movement, farming, remote firing or HTTP calls.

    Executor with writefile/makefolder recommended for JSON export.
    Only data replicated to YOUR client is observable. Run once per Sea,
    and repeat after moving to different areas if Workspace streaming is on.
]]

-- =================== CONFIG ===================
local OPTIONS = {
    FolderPrefix = "BloxFruits_Data",
    MaxInstanceRecords = 150000,      -- hard stop: mobile-friendly safety limit
    ChunkSize = 220,                  -- JSON records per file
    YieldEvery = 120,                 -- avoid long frozen frames
    ObserveSeconds = 35,             -- NPC + movement observation after scan (0 = disable)
    TrackMovementDiagnostics = true, -- read-only; no anti-cheat bypass or hooks
    MovementSampleInterval = 0.20,  -- lightweight position sampling in seconds
    LargeMovementStepStuds = 120,  -- log suspicious jumps, not regular 350-stud/s tween
    ReturnToleranceStuds = 25,      -- possible snapback near earlier position
    MovementMaxEvents = 350,
    WatchMaxHumanoids = 350,
    WatchMaxEvents = 1800,
    ExportAssets = true,             -- MeshId, TextureId, SoundId, AnimationId
    IncludeAttributes = true,
    IncludeTags = true,
    IncludeOwnCharacter = true,
    IncludeOwnInventory = true,
    IncludeOwnData = true,           -- only local player's ValueBase stats, not other players
    IncludeStarterGui = true,
    IncludeStarterPack = true,
    IncludeReplicatedFirst = true,
    IncludeSoundService = true,
    IncludeLighting = true,
    IncludeTeams = true,
    -- For safety, ModuleScripts are CATALOGUED but NEVER required/executed.
    -- Script/LocalScript.Source and private server objects are NOT accessed.
}

-- =================== BOOT =====================
local gameRef = game
local Players = gameRef:GetService("Players")
local Workspace = gameRef:GetService("Workspace")
local RS = gameRef:GetService("ReplicatedStorage")
local Http = gameRef:GetService("HttpService")
local CollectionService = gameRef:GetService("CollectionService")
local LocalPlayer = Players.LocalPlayer
local env = (type(getgenv) == "function" and getgenv()) or _G
local WORLD_IDS = {
    [2753915549] = 1, [85211729168715] = 1,
    [4442272183] = 2, [79091703265657] = 2,
    [7449423635] = 3, [100117331123089] = 3
}
local WORLD = WORLD_IDS[gameRef.PlaceId] or "unknown"
local RUN_TAG = os.date("%Y%m%d_%H%M%S") .. "_" .. tostring(Http:GenerateGUID(false)):sub(1,6)
local OUTPUT = OPTIONS.FolderPrefix .. "_Sea" .. tostring(WORLD) .. "_" .. RUN_TAG
local RUN = {alive=true, started=os.clock(), files={}, counts={}, errors={},
    categories={}, connections={}, scanned=0, truncated=false, rootCounts={},
    hiddenPlayers=0, observed={}, watchedHumanoids=0, old=nil,
    mobTypes={}, spawnTypes={}, deadModels=0, liveModels=0,
    movementEvents={}, movement={samples=0,maxObservedStudsPerSecond=0,
      maxSingleStepStuds=0, largeJumps=0, possibleSnapbacks=0,
      respawns=0, deaths=0, forcedAnchors=0, teleportErrors=0, stateChanges=0},
    movementConnections=0}

if type(env.__BF_GAME_DATA_COLLECTOR) == "table" and type(env.__BF_GAME_DATA_COLLECTOR.Stop) == "function" then
    pcall(env.__BF_GAME_DATA_COLLECTOR.Stop)
end
env.__BF_GAME_DATA_COLLECTOR = RUN

local function warnInfo(s)
    warn("[BF DATA] " .. tostring(s))
end
local function note(s)
    print("[BF DATA] " .. tostring(s))
end
local function recordError(where, err)
    if #RUN.errors>=250 then return end
    RUN.errors[#RUN.errors + 1] = {at=tostring(where), error=tostring(err)}
    if #RUN.errors <= 12 then warnInfo(tostring(where) .. ": " .. tostring(err)) end
end
local function getApi(name)
    if type(env[name]) == "function" then return env[name] end
    if type(_G[name]) == "function" then return _G[name] end
    return nil
end
local WRITE = getApi("writefile")
local MAKEFOLDER = getApi("makefolder")
local ISFOLDER = getApi("isfolder")
local CLIP = getApi("setclipboard") or getApi("toclipboard")
local canSave = WRITE ~= nil
if canSave and MAKEFOLDER then
    local ok, err = pcall(function()
        if not ISFOLDER or not ISFOLDER(OUTPUT) then MAKEFOLDER(OUTPUT) end
    end)
    if not ok then recordError("create output folder",err); canSave=false end
end
if canSave and not MAKEFOLDER then
    -- Some executors can write only to existing folders: use flat filenames.
    OUTPUT = OPTIONS.FolderPrefix .. "_Sea" .. tostring(WORLD) .. "_" .. RUN_TAG
end

local function filePath(basename)
    if MAKEFOLDER then return OUTPUT .. "/" .. basename end
    return OUTPUT .. "_" .. basename
end
local function safeWrite(name, value)
    if not canSave then return false end
    local ok, json = pcall(Http.JSONEncode, Http, value)
    if not ok then recordError("json: "..name,json); return false end
    local path = filePath(name)
    local wrote, err = pcall(WRITE,path,json)
    if not wrote then recordError("writefile: "..path,err); return false end
    RUN.files[#RUN.files+1] = {file=path, bytes=#json, kind=name}
    return true
end
local buffers = {}
local chunkSeq = {}
local categoryPreview = {}
local function flushCategory(kind)
    local b = buffers[kind]
    if not b or #b == 0 then return end
    chunkSeq[kind] = (chunkSeq[kind] or 0) + 1
    local filename = string.format("%s_%04d.json",kind,chunkSeq[kind])
    safeWrite(filename,b)
    buffers[kind] = {}
end
local function emit(kind, row)
    RUN.counts[kind] = (RUN.counts[kind] or 0) + 1
    if not categoryPreview[kind] then categoryPreview[kind]=row end
    if not canSave then return end
    local b = buffers[kind]
    if not b then b={}; buffers[kind]=b end
    b[#b+1] = row
    if #b >= OPTIONS.ChunkSize then flushCategory(kind) end
end
local function safeValue(fn, fallback)
    local ok, result = pcall(fn)
    if ok then return result end
    return fallback
end
local function shortPath(inst)
    return safeValue(function() return inst:GetFullName() end, tostring(inst.Name))
end
local function v3(v)
    if not v then return nil end
    return {x=math.floor(v.X*100+0.5)/100,
            y=math.floor(v.Y*100+0.5)/100,
            z=math.floor(v.Z*100+0.5)/100}
end
local function cfData(cf)
    if not cf then return nil end
    local parts={cf:GetComponents()}
    for i,n in ipairs(parts) do parts[i]=math.floor(n*10000+0.5)/10000 end
    return {position=v3(cf.Position), components=parts}
end
local function compactString(s, n)
    s = tostring(s)
    if #s <= (n or 2000) then return s end
    -- Avoid cutting in the middle of a UTF-8 character when possible.
    local cut = n or 2000
    local ok, u = pcall(function() return utf8.offset(s, 1, cut) end)
    if ok and type(u)=="number" then cut=math.min(cut,u-1) end
    return s:sub(1,cut) .. " [truncated]"
end
local function serial(v, depth, visited)
    depth = depth or 0
    visited = visited or {}
    if depth > 4 then return "[max-depth]" end
    local kind = typeof(v)
    if kind=="nil" then return nil end
    if kind=="string" then return compactString(v,1500) end
    if kind=="boolean" then return v end
    if kind=="number" then
        if v~=v or v==math.huge or v==-math.huge then return tostring(v) end
        return v
    end
    if kind=="Vector3" then return {type="Vector3",value=v3(v)} end
    if kind=="Vector2" then return {type="Vector2",x=v.X,y=v.Y} end
    if kind=="CFrame" then return {type="CFrame",value=cfData(v)} end
    if kind=="Color3" then return {type="Color3",r=v.R,g=v.G,b=v.B} end
    if kind=="UDim2" then return {type="UDim2",xs=v.X.Scale,xo=v.X.Offset,ys=v.Y.Scale,yo=v.Y.Offset} end
    if kind=="UDim" then return {type="UDim",scale=v.Scale,offset=v.Offset} end
    if kind=="NumberRange" then return {type="NumberRange",min=v.Min,max=v.Max} end
    if kind=="BrickColor" then return tostring(v) end
    if kind=="EnumItem" then return tostring(v) end
    if kind=="Instance" then return {type="Instance",class=v.ClassName,path=shortPath(v)} end
    if kind=="table" then
        if visited[v] then return "[circular]" end
        visited[v]=true
        local out={}
        local n=0
        for key,value in pairs(v) do
            n=n+1
            if n>100 then out.__truncated="100 entries";break end
            local k=type(key)=="string" and key or tostring(key)
            out[k]=serial(value,depth+1,visited)
        end
        visited[v]=nil
        return out
    end
    return "["..tostring(kind).."]"
end

-- =================== READ-ONLY SNAPSHOT HELPERS ===================
local function humanoid(model)
    return model and model:FindFirstChildOfClass("Humanoid") or nil
end
local function modelRoot(model)
    if not model then return nil end
    return model:FindFirstChild("HumanoidRootPart") or model.PrimaryPart or model:FindFirstChildWhichIsA("BasePart")
end
local function getProp(obj,key)
    return safeValue(function() return obj[key] end,nil)
end
local function isLiveNPC(model)
    local h=humanoid(model)
    return h and h.Health>0 and h.MaxHealth>0 and modelRoot(model)~=nil
end
local function shallowObj(inst)
    local data={name=inst.Name,class=inst.ClassName,path=shortPath(inst)}
    if OPTIONS.IncludeAttributes then
        local attrs=safeValue(function() return inst:GetAttributes() end,{})
        local result={}
        for k,v in pairs(attrs) do result[tostring(k)]=serial(v) end
        if next(result) then data.attributes=result end
    end
    if OPTIONS.IncludeTags then
        local tags=safeValue(function() return CollectionService:GetTags(inst) end,nil)
        if tags and #tags>0 then data.tags=tags end
    end
    if inst:IsA("BasePart") then
        data.position=v3(inst.Position)
        data.size=v3(inst.Size)
        data.cframe=cfData(inst.CFrame)
        data.anchored=inst.Anchored
        data.collidable=inst.CanCollide
        data.canTouch=inst.CanTouch
        data.transparency=inst.Transparency
        data.material=tostring(inst.Material)
        if inst:IsA("MeshPart") then
            data.meshId=getProp(inst,"MeshId")
            data.textureId=getProp(inst,"TextureID")
        end
    elseif inst:IsA("Model") then
        data.pivot=safeValue(function() return cfData(inst:GetPivot()) end,nil)
        data.primaryPart=inst.PrimaryPart and shortPath(inst.PrimaryPart) or nil
        local h=humanoid(inst)
        if h then
            data.humanoid={health=h.Health,maxHealth=h.MaxHealth,alive=h.Health>0,
                displayName=h.DisplayName,walkSpeed=h.WalkSpeed}
        end
    elseif inst:IsA("Humanoid") then
        data.health=inst.Health
        data.maxHealth=inst.MaxHealth
        data.walkSpeed=inst.WalkSpeed
        data.displayName=inst.DisplayName
    elseif inst:IsA("ValueBase") then
        data.value=serial(getProp(inst,"Value"))
    elseif inst:IsA("Tool") then
        data.tooltip=compactString(getProp(inst,"ToolTip") or "",300)
        data.canBeDropped=getProp(inst,"CanBeDropped")
        data.requiresHandle=getProp(inst,"RequiresHandle")
    elseif inst:IsA("ProximityPrompt") then
        data.enabled=inst.Enabled
        data.actionText=inst.ActionText
        data.objectText=inst.ObjectText
        data.holdDuration=inst.HoldDuration
        data.maxActivationDistance=inst.MaxActivationDistance
        data.requiresLineOfSight=inst.RequiresLineOfSight
    elseif inst:IsA("ClickDetector") then
        data.maxActivationDistance=inst.MaxActivationDistance
    elseif inst:IsA("Animation") then
        data.animationId=inst.AnimationId
    elseif inst:IsA("Sound") then
        data.soundId=inst.SoundId
        data.volume=inst.Volume
        data.looped=inst.Looped
    elseif inst:IsA("Decal") or inst:IsA("Texture") then
        data.textureId=inst.Texture
    elseif inst:IsA("SpecialMesh") then
        data.meshId=inst.MeshId
        data.textureId=inst.TextureId
    elseif inst:IsA("TextLabel") or inst:IsA("TextButton") then
        -- Deliberately do NOT read UI text in bulk to avoid personal chat data.
        data.visible=inst.Visible
    elseif inst:IsA("RemoteEvent") or inst:IsA("RemoteFunction") or
        inst:IsA("UnreliableRemoteEvent") then
        data.remoteType=inst.ClassName -- never FireServer/InvokeServer!
    elseif inst:IsA("ModuleScript") then
        data.note="Source is not readable by a normal game client; not required."
    end
    return data
end
local function getWorldOrigin()
    return Workspace:FindFirstChild("_WorldOrigin")
end
local function snapshotEnemy(model)
    if not model:IsA("Model") then return nil end
    local h=humanoid(model)
    local root=modelRoot(model)
    local aliveNow=isLiveNPC(model) and true or false
    local group=RUN.mobTypes[model.Name]
    if not group then
        group={name=model.Name,seen=0,alive=0,deadOrIncomplete=0,examplePositions={}}
        RUN.mobTypes[model.Name]=group
    end
    group.seen=group.seen+1
    if aliveNow then group.alive=group.alive+1;RUN.liveModels=RUN.liveModels+1
    else group.deadOrIncomplete=group.deadOrIncomplete+1;RUN.deadModels=RUN.deadModels+1 end
    if root and #group.examplePositions<4 then
        group.examplePositions[#group.examplePositions+1]=v3(root.Position)
    end
    return {name=model.Name,path=shortPath(model),position=root and v3(root.Position) or nil,
        hasHumanoid=h~=nil,health=h and h.Health or nil,
        maxHealth=h and h.MaxHealth or nil,
        alive=aliveNow,
        humanoidState=h and tostring(h:GetState()) or nil,
        isBossByName=string.find(string.lower(model.Name),"boss",1,true)~=nil}
end
local function isOtherPlayerCharacter(inst)
    if not inst:IsA("Model") then return false end
    local p=safeValue(function() return Players:GetPlayerFromCharacter(inst) end,nil)
    return p~=nil and p~=LocalPlayer
end
local function inFolder(inst, folder)
    return folder and inst~=folder and inst:IsDescendantOf(folder)
end
local function matchesName(inst,pattern)
    return string.find(string.lower(inst.Name),pattern)~=nil
end
local function classEventMatch(obj)
    if not (obj:IsA("Model") or obj:IsA("BasePart") or obj:IsA("Folder")) then return false end
    local name=string.lower(obj.Name)
    local terms={"halloween","pumpkin","ghost","event","dungeon","leviathan","terror",
        "sea beast","shark","raid","factory","dragon","kitsune","island","soul reaper",
        "dough king","sweet chalice","elite","rip_indra","tiki","celestial"}
    for _,term in ipairs(terms) do if name:find(term,1,true) then return true end end
    return false
end

-- =================== LIVE QUEST / OWN DATA ===================
local function snapshotSelf()
    if not LocalPlayer then
        emit("my_character",{note="LocalPlayer unavailable"})
        return
    end
    local own={team=LocalPlayer.Team and LocalPlayer.Team.Name or nil,
        characterLoaded=LocalPlayer.Character~=nil,placeId=gameRef.PlaceId,sea=WORLD}
    local character=LocalPlayer.Character
    local root=character and character:FindFirstChild("HumanoidRootPart")
    local h=character and humanoid(character)
    if root then own.position=v3(root.Position);own.cframe=cfData(root.CFrame) end
    if h then own.health=h.Health;own.maxHealth=h.MaxHealth end
    emit("my_character",own)
    if OPTIONS.IncludeOwnInventory then
        for _,container in ipairs({LocalPlayer:FindFirstChild("Backpack"),character}) do
            if container then
                for _,child in ipairs(container:GetChildren()) do
                    if child:IsA("Tool") then
                        emit("inventory_tools",{name=child.Name,path=shortPath(child),
                            container=container==character and "equipped" or "backpack",
                            tooltip=getProp(child,"ToolTip"),attributes=serial(safeValue(function() return child:GetAttributes() end,{}))})
                    end
                end
            end
        end
    end
    if OPTIONS.IncludeOwnData then
        local data=LocalPlayer:FindFirstChild("Data")
        if data then
            local queue={data};local head=1
            while head<=#queue do
                local v=queue[head];head=head+1
                if v:IsA("ValueBase") then
                    emit("my_stats",{name=v.Name,path=shortPath(v),value=serial(getProp(v,"Value"))})
                end
                if head<=250 then
                    for _,ch in ipairs(v:GetChildren()) do queue[#queue+1]=ch end
                end
            end
        else emit("my_stats",{note="LocalPlayer.Data not loaded"}) end
    end
end
local function snapshotQuestUI()
    local pg=LocalPlayer and LocalPlayer:FindFirstChild("PlayerGui")
    local main=pg and pg:FindFirstChild("Main")
    local quest=main and (main:FindFirstChild("Quest") or main:FindFirstChild("QuestContainer"))
    if not quest then
        emit("quest_ui",{found=false,note="No quest GUI loaded at scan time"})
        return
    end
    emit("quest_ui",{found=true,name=quest.Name,path=shortPath(quest),visible=getProp(quest,"Visible")})
    local total=0
    for _,ch in ipairs(quest:GetDescendants()) do
        if ch:IsA("TextLabel") or ch:IsA("TextButton") then
            total=total+1
            if total<=100 then
                emit("quest_ui",{name=ch.Name,path=shortPath(ch),text=compactString(ch.Text,600),
                    visible=ch.Visible})
            end
        end
    end
end

-- =================== NPC EVENT WATCHER =====================
local enemyFolder=Workspace:FindFirstChild("Enemies")
local watchedHumanoids=setmetatable({},{__mode="k"})
local function eventWatch(evt, data)
    if not RUN.alive or #RUN.observed>=OPTIONS.WatchMaxEvents then return end
    RUN.observed[#RUN.observed+1]={elapsed=math.floor((os.clock()-RUN.started)*100)/100,
        event=evt,data=data}
end
local bindEnemy
bindEnemy = function(model)
    if not RUN.alive or RUN.watchedHumanoids>=OPTIONS.WatchMaxHumanoids then return end
    if not model:IsA("Model") then return end
    local h=humanoid(model)
    if not h then
        -- The Humanoid may be inserted after the NPC model appears.
        local retry
        retry=model.ChildAdded:Connect(function(obj)
            if obj:IsA("Humanoid") then
                retry:Disconnect()
                bindEnemy(model)
            end
        end)
        RUN.connections[#RUN.connections+1]=retry
        return
    end
    if watchedHumanoids[h] then return end
    watchedHumanoids[h]=true
    RUN.watchedHumanoids=RUN.watchedHumanoids+1
    local lastHealth=h.Health
    local signal=h.HealthChanged:Connect(function(value)
        if not RUN.alive then return end
        if value<=0 and lastHealth>0 then
            eventWatch("npc_dead",{name=model.Name,path=shortPath(model),position=modelRoot(model) and v3(modelRoot(model).Position) or nil})
        elseif lastHealth<=0 and value>0 then
            eventWatch("npc_revived",{name=model.Name,path=shortPath(model)})
        end
        lastHealth=value
    end)
    RUN.connections[#RUN.connections+1]=signal
end
-- =================== MOVEMENT / POSSIBLE CORRECTION DIAGNOSTICS ==============
-- This monitors ONLY replicated client state and documented events.
-- It cannot identify server anti-cheat logic or bypass movement validation.
local function movementEvent(kind,data)
    if not RUN.alive or #RUN.movementEvents >= OPTIONS.MovementMaxEvents then return end
    local row={elapsed=math.floor((os.clock()-RUN.started)*100)/100,
        event=kind,data=data or {}}
    RUN.movementEvents[#RUN.movementEvents+1]=row
end
local function installMovementWatch()
    if not OPTIONS.TrackMovementDiagnostics or not LocalPlayer then return end
    local boundChar=setmetatable({},{__mode="k"})
    local function bindCharacter(char)
        if not char or boundChar[char] or not RUN.alive then return end
        boundChar[char]=true
        local hum=char:FindFirstChildOfClass("Humanoid")
        local root=char:FindFirstChild("HumanoidRootPart")
        if hum then
            local died=hum.Died:Connect(function()
                if not RUN.alive then return end
                RUN.movement.deaths=RUN.movement.deaths+1
                movementEvent("own_character_died",{health=hum.Health})
            end)
            RUN.connections[#RUN.connections+1]=died
            local significant={Dead=true,PlatformStanding=true,Ragdoll=true,FallingDown=true,
                Seated=true,Physics=true}
            RUN.connections[#RUN.connections+1]=hum.StateChanged:Connect(function(oldState,newState)
                if not RUN.alive then return end
                local state=tostring(newState):match("%.([^.]+)$") or tostring(newState)
                if significant[state] then
                    RUN.movement.stateChanges=RUN.movement.stateChanges+1
                    movementEvent("humanoid_state",{from=tostring(oldState),to=tostring(newState)})
                end
            end)
        end
        if root then
            RUN.connections[#RUN.connections+1]=root:GetPropertyChangedSignal("Anchored"):Connect(function()
                if RUN.alive then
                    if root.Anchored then RUN.movement.forcedAnchors=RUN.movement.forcedAnchors+1 end
                    movementEvent("root_anchored_changed",{anchored=root.Anchored,
                        position=v3(root.Position)})
                end
            end)
        end
    end
    if LocalPlayer.Character then bindCharacter(LocalPlayer.Character) end
    RUN.connections[#RUN.connections+1]=LocalPlayer.CharacterAdded:Connect(function(char)
        if not RUN.alive then return end
        RUN.movement.respawns=RUN.movement.respawns+1
        movementEvent("character_added",{note="Respawn or character reload"})
        task.defer(function()
            if not RUN.alive then return end
            pcall(function() char:WaitForChild("HumanoidRootPart",5) end)
            if RUN.alive then bindCharacter(char) end
        end)
    end)
    -- Documented teleport failure event. No teleport is requested here.
    pcall(function()
        local TP=gameRef:GetService("TeleportService")
        RUN.connections[#RUN.connections+1]=TP.TeleportInitFailed:Connect(function(player,result,errorMessage,placeId)
            if not RUN.alive or player~=LocalPlayer then return end
            RUN.movement.teleportErrors=RUN.movement.teleportErrors+1
            movementEvent("teleport_init_failed",{result=tostring(result),placeId=placeId,
                reason=compactString(errorMessage or "",220)})
        end)
    end)
    task.spawn(function()
        local prev=nil
        local history={}
        local lastSamplingRecord=0
        local lastRoot=nil
        while RUN.alive do
            local char=LocalPlayer.Character
            local root=char and char:FindFirstChild("HumanoidRootPart")
            local hum=char and char:FindFirstChildOfClass("Humanoid")
            local now=os.clock()
            if root then
                if root~=lastRoot then prev=nil;history={};lastRoot=root end
                local position=root.Position
                if prev then
                    local dt=now-prev.t
                    if dt>=.04 then
                        local distance=(position-prev.pos).Magnitude
                        local speed=distance/dt
                        local M=RUN.movement
                        M.samples=M.samples+1
                        M.maxObservedStudsPerSecond=math.max(M.maxObservedStudsPerSecond,speed)
                        M.maxSingleStepStuds=math.max(M.maxSingleStepStuds,distance)
                        local limit=OPTIONS.LargeMovementStepStuds
                        -- Scale threshold with sample delay so low FPS doesn't create false positives.
                        local adaptiveLimit=math.max(limit,limit*dt/.2)
                        local snapback=false
                        if distance>=40 then
                            for i=#history,1,-1 do
                                local h=history[i]
                                local age=now-h.t
                                if age>6 then break end
                                if age>.7 and (position-h.pos).Magnitude<OPTIONS.ReturnToleranceStuds then
                                    snapback=true
                                    break
                                end
                            end
                        end
                        if distance >= adaptiveLimit then M.largeJumps=M.largeJumps+1 end
                        if snapback or distance >= adaptiveLimit then
                            if snapback then M.possibleSnapbacks=M.possibleSnapbacks+1 end
                            movementEvent(snapback and "possible_snapback" or "large_position_step",{
                                from=v3(prev.pos),to=v3(position),stepStuds=math.floor(distance*10)/10,
                                sampleSeconds=math.floor(dt*100)/100,approxSpeed=math.floor(speed),
                                humanoidState=hum and tostring(hum:GetState()) or "unknown",
                                note="Possible correction / teleport / lag. Not proof of anti-cheat."})
                        end
                        if now-lastSamplingRecord>=2 then
                            lastSamplingRecord=now
                            movementEvent("movement_sample",{position=v3(position),
                                velocity=v3(root.AssemblyLinearVelocity),
                                anchored=root.Anchored,
                                state=hum and tostring(hum:GetState()) or "unknown"})
                        end
                    end
                end
                prev={pos=position,t=now}
                history[#history+1]={pos=position,t=now}
                if #history>34 then table.remove(history,1) end
            else
                prev=nil
                lastRoot=nil
                history={}
            end
            task.wait(math.max(.12,OPTIONS.MovementSampleInterval))
        end
    end)
end
local function installWatch()
    if OPTIONS.ObserveSeconds<=0 then return end
    installMovementWatch()
    if enemyFolder then
        for _,m in ipairs(enemyFolder:GetChildren()) do bindEnemy(m) end
        RUN.connections[#RUN.connections+1] = enemyFolder.ChildAdded:Connect(function(m)
            eventWatch("enemy_added",{name=m.Name,path=shortPath(m)})
            bindEnemy(m)
        end)
        RUN.connections[#RUN.connections+1] = enemyFolder.ChildRemoved:Connect(function(m)
            eventWatch("enemy_removed",{name=m.Name,path=shortPath(m)})
        end)
    end
    -- Only subscribe to specifically named quest remote events. Never fire a remote.
    for _,obj in ipairs(RS:GetDescendants()) do
        if obj:IsA("RemoteEvent") then
            local lname=string.lower(obj.Name)
            if lname=="questupdate" or lname=="questchanged" or
                lname=="queststatus" or lname=="updatequest" then
                RUN.connections[#RUN.connections+1] = obj.OnClientEvent:Connect(function(...)
                    local args={...}
                    eventWatch("quest_remote",{path=shortPath(obj),arguments=serial(args)})
                end)
            end
        end
    end
end
local function stop()
    if not RUN.alive then return end
    RUN.alive=false
    for _,c in ipairs(RUN.connections) do pcall(function() c:Disconnect() end) end
    RUN.connections={}
end
RUN.Stop=stop

-- =================== INSTANCE TREE SCANNER ===================
local origin=getWorldOrigin()
local enemySpawn=origin and origin:FindFirstChild("EnemySpawns")
local playerSpawn=origin and origin:FindFirstChild("PlayerSpawns")
local locations=origin and origin:FindFirstChild("Locations")
local map=Workspace:FindFirstChild("Map")
local npcs=Workspace:FindFirstChild("NPCs")
local boats=Workspace:FindFirstChild("Boats")
local function scan(root,label)
    if not root or not RUN.alive then return end
    local q={[1]=root}
    local head,tail=1,1
    local scannedThisRoot=0
    while head<=tail and RUN.alive do
        if RUN.scanned>=OPTIONS.MaxInstanceRecords then RUN.truncated=true;break end
        local obj=q[head];q[head]=nil;head=head+1
        if obj and obj.Parent or obj==root then
            if not isOtherPlayerCharacter(obj) then
                RUN.scanned=RUN.scanned+1
                scannedThisRoot=scannedThisRoot+1
                local row=safeValue(function() return shallowObj(obj) end,nil)
                if not row then
                    recordError("instance read: "..shortPath(obj),"Object became unavailable during scan")
                    row={name=obj.Name,class=obj.ClassName,path=shortPath(obj),error="read failed"}
                end
                emit("tree_"..label,row)

                if obj:IsA("RemoteEvent") or obj:IsA("RemoteFunction") or
                    obj:IsA("UnreliableRemoteEvent") then
                    emit("remotes",row)
                elseif obj:IsA("ModuleScript") then
                    emit("modules",{name=obj.Name,path=shortPath(obj),note="not executed"})
                    if obj.Name:lower():find("quest",1,true) or obj.Name:lower():find("guide",1,true) then
                        emit("quest_sources",{name=obj.Name,path=shortPath(obj),kind="ModuleScript metadata"})
                    end
                elseif obj:IsA("Tool") then
                    emit("tools",row)
                elseif obj:IsA("ProximityPrompt") or obj:IsA("ClickDetector") then
                    emit("interactions",row)
                elseif OPTIONS.ExportAssets and (obj:IsA("Animation") or obj:IsA("Sound") or
                    obj:IsA("Decal") or obj:IsA("Texture") or obj:IsA("SpecialMesh") or obj:IsA("MeshPart")) then
                    emit("assets",row)
                end

                if label=="Workspace" then
                    if obj:IsA("Model") and enemyFolder and obj.Parent==enemyFolder then
                        emit("enemies",snapshotEnemy(obj))
                    end
                    if npcs and inFolder(obj,npcs) and obj:IsA("Model") then
                        local h=humanoid(obj)
                        if h or obj.Parent==npcs then
                            emit("npcs",{name=obj.Name,path=shortPath(obj),pivot=row.pivot,
                                hasHumanoid=h~=nil,alive=isLiveNPC(obj) and true or false})
                        end
                    end
                    if inFolder(obj,enemySpawn) and obj:IsA("BasePart") then
                        local group=RUN.spawnTypes[obj.Name]
                        if not group then
                            group={name=obj.Name,count=0,examplePositions={}}
                            RUN.spawnTypes[obj.Name]=group
                        end
                        group.count=group.count+1
                        if #group.examplePositions<5 then group.examplePositions[#group.examplePositions+1]=row.position end
                        emit("enemy_spawns",{name=obj.Name,path=shortPath(obj),position=row.position,cframe=row.cframe})
                    end
                    if inFolder(obj,playerSpawn) and obj:IsA("BasePart") then
                        emit("player_spawns",{name=obj.Name,path=shortPath(obj),position=row.position})
                    end
                    if locations and obj.Parent==locations then
                        emit("locations",{name=obj.Name,path=shortPath(obj),position=row.position or
                            (row.pivot and row.pivot.position),cframe=row.cframe or row.pivot})
                    end
                    if map and obj.Parent==map then
                        emit("map_top_level",{name=obj.Name,class=obj.ClassName,path=shortPath(obj),
                            position=row.position or (row.pivot and row.pivot.position)})
                    end
                    if boats and inFolder(obj,boats) and
                        (obj:IsA("Model") or obj:IsA("VehicleSeat")) then emit("boats",row) end
                    if classEventMatch(obj) then emit("events_and_landmarks",row) end
                end

                local children=safeValue(function() return obj:GetChildren() end,{})
                for _,ch in ipairs(children) do tail=tail+1;q[tail]=ch end
                if RUN.scanned%OPTIONS.YieldEvery==0 then task.wait() end
            else
                RUN.hiddenPlayers=RUN.hiddenPlayers+1
            end
        end
    end
    RUN.rootCounts[label]=scannedThisRoot
    note(label .. " -> " .. scannedThisRoot .. " objects")
end

-- =================== OUTPUT / MANIFEST ==================
local function makeManifest()
    local mobTypeCount=0
    for _ in pairs(RUN.mobTypes) do mobTypeCount=mobTypeCount+1 end
    local catNames={}
    for kind,count in pairs(RUN.counts) do catNames[#catNames+1]={category=kind,records=count,chunks=chunkSeq[kind] or 0} end
    table.sort(catNames,function(a,b) return a.category<b.category end)
    return {
        project="Blox Fruits Game Data Collector",
        version="2.0.0",
        snapshotDateUTC=os.date("!%Y-%m-%dT%H:%M:%SZ"),
        placeId=gameRef.PlaceId,
        gameId=gameRef.GameId,
        jobId=gameRef.JobId,
        sea=WORLD,
        durationSeconds=math.floor((os.clock()-RUN.started)*100)/100,
        clientStreamingWarning="Workspace scan may omit unloaded distant locations",
        serverOnlyWarning="No access to ServerScriptService/ServerStorage, private code or server-only state",
        modulesWarning="ModuleScript names only. No module source read or require executed",
        noNetworkRequests=true,
        noRemoteCalls=true,
        antiCheatBypass=false,
        telemetryOnly=true,
        antiCheatWarning="Movement jumps, snapbacks or disconnections are observations only, not proof of server anti-cheat actions",
        movementSummary=RUN.movement,
        ownDataOnly=true,
        truncated=RUN.truncated,
        maxInstanceRecords=OPTIONS.MaxInstanceRecords,
        objectsScanned=RUN.scanned,
        otherPlayerCharactersSkipped=RUN.hiddenPlayers,
        liveEnemyModelsAtScan=RUN.liveModels,
        deadOrIncompleteEnemyModelsAtScan=RUN.deadModels,
        uniqueEnemyTypes=mobTypeCount,
        rootCounts=RUN.rootCounts,
        categoryStats=catNames,
        files=RUN.files,
        errors=RUN.errors,
        settings={observeSeconds=OPTIONS.ObserveSeconds,chunkSize=OPTIONS.ChunkSize,
            yieldEvery=OPTIONS.YieldEvery,includeTags=OPTIONS.IncludeTags},
    }
end
local function finalise()
    local mobSummary={}
    for _,entry in pairs(RUN.mobTypes) do mobSummary[#mobSummary+1]=entry end
    table.sort(mobSummary,function(a,b) return a.name<b.name end)
    local spawnSummary={}
    for _,entry in pairs(RUN.spawnTypes) do spawnSummary[#spawnSummary+1]=entry end
    table.sort(spawnSummary,function(a,b) return a.name<b.name end)
    safeWrite("mob_summary.json",mobSummary)
    safeWrite("enemy_spawn_summary.json",spawnSummary)
    safeWrite("movement_summary.json",RUN.movement)
    if #RUN.movementEvents>0 then
        for _,r in ipairs(RUN.movementEvents) do emit("movement_events",r) end
    end
    for kind in pairs(buffers) do flushCategory(kind) end
    if #RUN.observed>0 then
        for _,r in ipairs(RUN.observed) do emit("observations",r) end
        flushCategory("observations")
    end
    local manifest=makeManifest()
    -- manifest should list all category data files; summary file itself is separate.
    safeWrite("manifest.json",manifest)
    local summary={
        "BLOX FRUITS DATA COLLECTOR 2.0",
        "Sea: "..tostring(WORLD).." | PlaceId: "..tostring(gameRef.PlaceId),
        "Captured objects: "..tostring(RUN.scanned),
        "Truncated: "..tostring(RUN.truncated),
        "Export path: "..OUTPUT,
        "JSON files written: "..tostring(#RUN.files),
        "Quest/UI update events: see observations_*.json",
        "Movement diagnostics: see movement_summary.json and movement_events_*.json",
        "Potential snapbacks: "..tostring(RUN.movement.possibleSnapbacks),
        "NOTE: Only client-replicated content from the current Sea.",
        "Re-run in Sea 1 / Sea 2 / Sea 3 and near distant islands for wider coverage."
    }
    for _,s in ipairs(summary) do note(s) end
    if not canSave then
        warnInfo("No writefile support. Full JSON cannot be saved in this executor.")
        if CLIP then
            local ok=pcall(CLIP,Http:JSONEncode({summary=summary,stats=manifest.categoryStats,
                preview=categoryPreview,errors=RUN.errors}))
            if ok then note("Summary copied to clipboard") end
        end
    else
        local ok,err=pcall(WRITE,filePath("SUMMARY.txt"),table.concat(summary,"\n"))
        if not ok then recordError("SUMMARY.txt",err) end
        note("Saved to: "..OUTPUT)
    end
end

-- =================== MAIN ====================
local function main()
    note("Starting read-only snapshot. World/Sea=" .. tostring(WORLD))
    if not LocalPlayer then
        recordError("start","LocalPlayer is missing")
        return
    end
    installWatch()
    snapshotSelf()
    snapshotQuestUI()

    -- Read replicated modules/remotes before the potentially huge Workspace tree.
    local roots={
        {"ReplicatedStorage",RS},
        {"Workspace",Workspace},
    }
    if OPTIONS.IncludeReplicatedFirst then
        roots[#roots+1]={"ReplicatedFirst",gameRef:GetService("ReplicatedFirst")}
    end
    if OPTIONS.IncludeLighting then roots[#roots+1]={"Lighting",gameRef:GetService("Lighting")} end
    if OPTIONS.IncludeStarterGui then roots[#roots+1]={"StarterGui",gameRef:GetService("StarterGui")} end
    if OPTIONS.IncludeStarterPack then roots[#roots+1]={"StarterPack",gameRef:GetService("StarterPack")} end
    if OPTIONS.IncludeSoundService then roots[#roots+1]={"SoundService",gameRef:GetService("SoundService")} end
    if OPTIONS.IncludeTeams then roots[#roots+1]={"Teams",gameRef:GetService("Teams")} end
    if OPTIONS.IncludeOwnCharacter and LocalPlayer.Character then
        roots[#roots+1]={"MyCharacter",LocalPlayer.Character}
    end
    for _,tuple in ipairs(roots) do
        if not RUN.alive or RUN.truncated then break end
        local ok,err=pcall(scan,tuple[2],tuple[1])
        if not ok then recordError("scan "..tuple[1],err) end
    end
    if RUN.alive and OPTIONS.ObserveSeconds>0 then
        local deadline=os.clock()+OPTIONS.ObserveSeconds
        note("Observation: waiting "..tostring(OPTIONS.ObserveSeconds).." seconds for NPC, quest and movement diagnostics")
        while RUN.alive and os.clock()<deadline do task.wait(.25) end
    end
end
local success,err=pcall(main)
if not success then recordError("main",err) end
stop()
local ok,finishErr=pcall(finalise)
if not ok then warnInfo("Export failed: "..tostring(finishErr)) end
if env.__BF_GAME_DATA_COLLECTOR==RUN then env.__BF_GAME_DATA_COLLECTOR=nil end
